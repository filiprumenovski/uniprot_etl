#!/usr/bin/env python3
import argparse
import os
import urllib.request
import sys
import ftplib
from urllib.parse import urlparse

BASE_URL_DIVISIONS = "ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/"
BASE_URL_COMPLETE = "ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/"

FTP_HOST = "ftp.uniprot.org"
FTP_DIR_DIVISIONS = "/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/"
FTP_DIR_COMPLETE = "/pub/databases/uniprot/current_release/knowledgebase/complete/"

DIVISIONS = [
    "archaea",
    "bacteria",
    "fungi",
    "human",
    "invertebrates",
    "mammals",
    "plants",
    "rodents",
    "vertebrates",
    "viruses",
]

# TrEMBL specific divisions
TREMBL_ONLY = [
    "unclassified"
]

def check_files(filenames, directory):
    print("Connecting to FTP...")
    try:
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login()
        print(f"Connected. Changing directory to {directory}...")
        ftp.cwd(directory)
        
        print("Listing files...")
        server_files = set(ftp.nlst())
        
        all_ok = True
        for filename in filenames:
            if filename in server_files:
                print(f"[OK] {filename}")
            else:
                print(f"[MISSING] {filename}")
                all_ok = False
        
        ftp.quit()
        return all_ok
    except Exception as e:
        print(f"FTP Error: {e}")
        return False

def download_file(url, dest_path):
    print(f"Downloading {url} to {dest_path}...")
    try:
        urllib.request.urlretrieve(url, dest_path)
        print(f"Finished {dest_path}")
    except Exception as e:
        print(f"Error downloading {url}: {e}", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(description="Download UniProt XML files.")
    parser.add_argument("dataset", choices=["sprot", "trembl", "trembl-fasta", "sprot-varsplic"], help="Dataset to download")
    parser.add_argument("--output-dir", required=True, help="Directory to save downloaded files")
    parser.add_argument("--check-only", action="store_true", help="Check if files exist without downloading")
    
    args = parser.parse_args()
    
    files_to_process = [] # list of (filename, url, ftp_dir)
    
    if args.dataset == "trembl-fasta":
        filename = "uniprot_trembl.fasta.gz"
        files_to_process.append((
            filename,
            f"{BASE_URL_COMPLETE}{filename}",
            FTP_DIR_COMPLETE
        ))
    elif args.dataset == "sprot-varsplic":
        filename = "uniprot_sprot_varsplic.fasta.gz"
        files_to_process.append((
            filename,
            f"{BASE_URL_COMPLETE}{filename}",
            FTP_DIR_COMPLETE
        ))
    else:
        # XML divisions
        divisions_to_download = DIVISIONS.copy()
        if args.dataset == "trembl":
            divisions_to_download.extend(TREMBL_ONLY)
        
        for division in divisions_to_download:
            filename = f"uniprot_{args.dataset}_{division}.xml.gz"
            files_to_process.append((
                filename,
                f"{BASE_URL_DIVISIONS}{filename}",
                FTP_DIR_DIVISIONS
            ))

    if args.check_only:
        # Group by directory for efficient checking
        files_by_dir = {}
        for filename, _, ftp_dir in files_to_process:
            if ftp_dir not in files_by_dir:
                files_by_dir[ftp_dir] = []
            files_by_dir[ftp_dir].append(filename)
            
        success = True
        for directory, filenames in files_by_dir.items():
            if not check_files(filenames, directory):
                success = False
        
        if not success:
            sys.exit(1)
        return

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
        
    for filename, url, _ in files_to_process:
        dest_path = os.path.join(args.output_dir, filename)
        download_file(url, dest_path)

if __name__ == "__main__":
    main()
