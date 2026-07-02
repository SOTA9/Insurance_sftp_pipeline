from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

import gnupg


def generate_keypair(
    name: str,
    email: str,
    passphrase: str,
    output_dir: Path,
    dir_id: str | None = None,
    key_type: str = "RSA",
    key_length: int = 4096,
) -> tuple[Path, Path, str]:
    """
    Generate a PGP key pair.

    When dir_id is provided, files are named:
      {dir_id}_public_key.asc   : safe to commit
      /tmp/{dir_id}_private_key.asc : upload to Secret Manager

    When dir_id is None (global / backward compat):
      public_key.asc
      /tmp/insureflow_private_key.asc

    Returns (public_key_path, private_key_path, fingerprint).
    """
    gpg_home = Path(tempfile.mkdtemp(prefix="insureflow_gpg_keygen_"))
    gpg_home.chmod(0o700)

    try:
        gpg = gnupg.GPG(gnupghome=str(gpg_home))
        gpg.encoding = "utf-8"

        label = f"[{dir_id}]" if dir_id else "[global]"
        print(f"{label} Generating {key_length}-bit {key_type} key for {name} <{email}> ...")

        input_data = gpg.gen_key_input(
            key_type=key_type,
            key_length=key_length,
            name_real=name,
            name_email=email,
            passphrase=passphrase,
            expire_date="2y",
        )
        key = gpg.gen_key(input_data)
        if not key.fingerprint:
            raise RuntimeError("Key generation failed — no fingerprint returned.")

        print(f"{label} Fingerprint: {key.fingerprint}")

        output_dir.mkdir(parents=True, exist_ok=True)

        # Public key filename - scoped to dir_id or global
        pub_filename = f"{dir_id}_public_key.asc" if dir_id else "public_key.asc"
        pub_path     = output_dir / pub_filename

        pub_asc = gpg.export_keys(key.fingerprint, armor=True)
        pub_path.write_text(pub_asc)
        print(f"{label} Public key - {pub_path}  safe to commit")

        # Private key - always to /tmp, never to the repo
        priv_filename = f"{dir_id}_private_key.asc" if dir_id else "insureflow_private_key.asc"
        priv_path     = Path("/tmp") / priv_filename

        priv_asc = gpg.export_keys(
            key.fingerprint, secret=True, armor=True, passphrase=passphrase,
        )
        priv_path.write_text(priv_asc)
        priv_path.chmod(0o600)
        print(f"{label} Private key → {priv_path}   DO NOT COMMIT — load to Secret Manager")

        # Write fingerprint for CI reference
        fp_filename = f"{dir_id}_fingerprint.txt" if dir_id else "fingerprint.txt"
        (output_dir / fp_filename).write_text(key.fingerprint + "\n")

        return pub_path, priv_path, key.fingerprint

    finally:
        shutil.rmtree(str(gpg_home), ignore_errors=True)


def generate_for_directory(sftp_dir: dict, output_dir: Path, passphrase: str) -> None:
    """
    Generate a key pair for one SFTP directory config dict from YAML.
    Only called for directories with use_pgp=True.
    """
    dir_id = sftp_dir["dir_id"]
    label  = sftp_dir.get("label", dir_id)

    print(f"\n{'─'*60}")
    print(f"Directory: {dir_id}  ({label})")
    print(f"{'─'*60}")

    pub, priv, fingerprint = generate_keypair(
        name=f"InsureFlow {dir_id.title()}",
        email=f"pipeline-{dir_id}@insureflow.io",
        passphrase=passphrase,
        output_dir=output_dir,
        dir_id=dir_id,
    )

    # Print the exact gcloud commands to load into Secret Manager
    print(f"\n{label} — load to Secret Manager:")
    print(f"\n  # Private key:")
    print(f"  gcloud secrets versions add {dir_id}-pgp-private-key \\")
    print(f"    --data-file={priv} \\")
    print(f"    --project=YOUR_PROJECT_ID")
    print(f"\n  # Passphrase:")
    print(f"  echo -n '{passphrase}' | \\")
    print(f"    gcloud secrets versions add {dir_id}-pgp-passphrase \\")
    print(f"    --data-file=- --project=YOUR_PROJECT_ID")
    print(f"\n  # After loading, delete the private key file:")
    print(f"  rm -f {priv}")
    print(f"\n  # Fingerprint for partner (they encrypt TO this key):")
    print(f"  {fingerprint}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate PGP key pairs for InsureFlow SFTP directories",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:

  # Generate global key pair (original behavior — backward compatible)
  python keys/generate_pgp_keypair.py

  # Generate key pair scoped to /inbound/ directory
  python keys/generate_pgp_keypair.py --dir-id inbound

  # Generate key pairs for ALL use_pgp=True directories from YAML
  python keys/generate_pgp_keypair.py --generate-all

  # CI usage - generate for inbound with CI passphrase
  python keys/generate_pgp_keypair.py \\
    --dir-id inbound \\
    --passphrase ci-test-passphrase \\
    --output-dir keys/
        """,
    )
    parser.add_argument(
        "--dir-id",
        default=None,
        help=(
            "SFTP directory ID from sftp_directories.yaml e.g. 'inbound'. "
            "When not provided generates the global key pair."
        ),
    )
    parser.add_argument(
        "--generate-all",
        action="store_true",
        help="Read sftp_directories.yaml and generate one key pair per use_pgp=True directory.",
    )
    parser.add_argument("--name",       default="InsureFlow DataPipeline")
    parser.add_argument("--email",      default="pipeline@insureflow.io")
    parser.add_argument("--passphrase", default="changeme-in-production")
    parser.add_argument("--output-dir", default="keys", type=Path)
    parser.add_argument(
        "--yaml",
        default="manifests/sftp_directories.yaml",
        type=Path,
        help="Path to sftp_directories.yaml (used with --generate-all)",
    )
    args = parser.parse_args()

    if args.generate_all:
        # Generate one key pair per encrypted directory from YAML
        import yaml

        if not args.yaml.exists():
            print(f"ERROR: {args.yaml} not found. Run from project root.")
            raise SystemExit(1)

        config       = yaml.safe_load(args.yaml.read_text())
        pgp_dirs     = [d for d in config.get("sftp_directories", [])
                        if d.get("use_pgp", False)]

        if not pgp_dirs:
            print("No directories with use_pgp=True found in YAML.")
            raise SystemExit(0)

        print(f"Found {len(pgp_dirs)} PGP-encrypted director(ies): "
              f"{[d['dir_id'] for d in pgp_dirs]}")

        for sftp_dir in pgp_dirs:
            generate_for_directory(sftp_dir, args.output_dir, args.passphrase)

        print(f"\n{'═'*60}")
        print("All key pairs generated.")
        print(f"Public keys in {args.output_dir}/ → commit these.")
        print("Private keys in /tmp/              → load to Secret Manager, then DELETE.")
        print(f"{'═'*60}")

    elif args.dir_id:
        # Generate for a specific directory
        pub, priv, fingerprint = generate_keypair(
            name=args.name,
            email=args.email,
            passphrase=args.passphrase,
            output_dir=args.output_dir,
            dir_id=args.dir_id,
        )
        print(f"\nNext steps for directory '{args.dir_id}':")
        print(f"  1. Commit {pub} to the repository.")
        print(f"  2. Run:")
        print(f"     gcloud secrets versions add {args.dir_id}-pgp-private-key \\")
        print(f"       --data-file={priv} --project=YOUR_PROJECT_ID")
        print(f"     echo -n '{args.passphrase}' | \\")
        print(f"       gcloud secrets versions add {args.dir_id}-pgp-passphrase \\")
        print(f"       --data-file=- --project=YOUR_PROJECT_ID")
        print(f"  3. Delete {priv} from disk.")
        print(f"  4. Share fingerprint {fingerprint} with the partner.")

    else:
        # Original behavior — global key pair
        pub, priv, fingerprint = generate_keypair(
            name=args.name,
            email=args.email,
            passphrase=args.passphrase,
            output_dir=args.output_dir,
            dir_id=None,
        )
        print(f"\nNext steps (global key):")
        print(f"  1. Commit {pub} to the repository.")
        print(f"  2. Run:")
        print(f"     gcloud secrets versions add pgp-private-key \\")
        print(f"       --data-file={priv} --project=YOUR_PROJECT_ID")
        print(f"     echo -n '{args.passphrase}' | \\")
        print(f"       gcloud secrets versions add pgp-passphrase \\")
        print(f"       --data-file=- --project=YOUR_PROJECT_ID")
        print(f"  3. Delete {priv} from disk.")


if __name__ == "__main__":
    main()