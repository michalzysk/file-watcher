#!/usr/bin/env python3
"""
SMB File Transfer Performance Test Script

This script tests file transfer performance from SMB shares using different Python libraries.
It includes timing measurements to compare against your Spring Integration performance.

Requirements:
    pip install smbprotocol pysmb

For fastest performance (optional):
    pip install smbclient
    # Requires libsmbclient-dev on Linux or samba-client tools
"""

import time
import os
import sys
from pathlib import Path

def test_smbprotocol(server, share, filepath, username, password, domain=""):
    """
    Test using smbprotocol library (modern SMB2/3 support)
    Generally good performance and reliability
    """
    try:
        from smbprotocol.connection import Connection
        from smbprotocol.session import Session
        from smbprotocol.tree import TreeConnect
        from smbprotocol.open import Open, CreateDisposition, FileAccessMask, CreateOptions
        from smbprotocol.file_info import FileStandardInformation
        
        print(f"Testing smbprotocol: {server}\\{share}\\{filepath}")
        start_time = time.time()
        
        # Create connection
        connection = Connection(uuid.uuid4(), server, 445)
        connection.connect()
        
        # Create session
        session = Session(connection, username, password, domain)
        session.connect()
        
        # Connect to tree (share)
        tree = TreeConnect(session, f"\\\\{server}\\{share}")
        tree.connect()
        
        # Open file
        file_open = Open(tree, filepath)
        file_open.create(
            CreateDisposition.FILE_OPEN,
            FileAccessMask.FILE_READ_DATA,
            CreateOptions.FILE_NON_DIRECTORY_FILE
        )
        
        # Get file size
        file_info = file_open.query_info(FileStandardInformation)
        file_size = file_info.end_of_file
        
        # Read file in chunks
        data = b""
        bytes_read = 0
        chunk_size = 64 * 1024  # 64KB chunks
        
        while bytes_read < file_size:
            remaining = min(chunk_size, file_size - bytes_read)
            chunk = file_open.read(remaining, bytes_read)
            data += chunk
            bytes_read += len(chunk)
            
            # Progress indicator
            if bytes_read % (1024 * 1024) == 0:  # Every MB
                print(f"  Read: {bytes_read / (1024*1024):.1f} MB")
        
        # Cleanup
        file_open.close()
        tree.disconnect()
        session.disconnect()
        connection.disconnect()
        
        end_time = time.time()
        duration = end_time - start_time
        speed_mbps = (len(data) / (1024 * 1024)) / duration
        
        print(f"✓ smbprotocol completed:")
        print(f"  Size: {len(data) / (1024*1024):.2f} MB")
        print(f"  Time: {duration:.2f} seconds")
        print(f"  Speed: {speed_mbps:.2f} MB/s")
        
        return data, duration
        
    except ImportError:
        print("❌ smbprotocol not installed. Install with: pip install smbprotocol")
        return None, None
    except Exception as e:
        print(f"❌ smbprotocol failed: {e}")
        return None, None


def test_pysmb(server, share, filepath, username, password, domain="WORKGROUP"):
    """
    Test using pysmb library (older but stable)
    Generally slower than smbprotocol but more compatible
    """
    try:
        from smb.SMBConnection import SMBConnection
        import tempfile
        
        print(f"Testing pysmb: {server}\\{share}\\{filepath}")
        start_time = time.time()
        
        # Create connection
        conn = SMBConnection(username, password, "python-client", server, domain=domain, use_ntlm_v2=True)
        
        # Connect
        if not conn.connect(server, 445):
            raise Exception("Failed to connect to server")
        
        # Create temporary file to store downloaded data
        with tempfile.NamedTemporaryFile() as temp_file:
            # Download file
            file_attributes, file_size = conn.retrieveFile(share, filepath, temp_file)
            
            # Read the data
            temp_file.seek(0)
            data = temp_file.read()
        
        conn.close()
        
        end_time = time.time()
        duration = end_time - start_time
        speed_mbps = (len(data) / (1024 * 1024)) / duration
        
        print(f"✓ pysmb completed:")
        print(f"  Size: {len(data) / (1024*1024):.2f} MB")
        print(f"  Time: {duration:.2f} seconds")
        print(f"  Speed: {speed_mbps:.2f} MB/s")
        
        return data, duration
        
    except ImportError:
        print("❌ pysmb not installed. Install with: pip install pysmb")
        return None, None
    except Exception as e:
        print(f"❌ pysmb failed: {e}")
        return None, None


def test_smbclient(server, share, filepath, username, password, domain=""):
    """
    Test using smbclient library (wrapper around native smbclient)
    Usually the fastest option but requires system dependencies
    """
    try:
        import smbclient
        
        print(f"Testing smbclient: {server}\\{share}\\{filepath}")
        start_time = time.time()
        
        # Configure authentication
        if domain:
            full_username = f"{domain}\\{username}"
        else:
            full_username = username
            
        smbclient.register_session(server, username=full_username, password=password)
        
        # Read file
        smb_path = f"\\\\{server}\\{share}\\{filepath}"
        with smbclient.open_file(smb_path, mode="rb") as f:
            data = f.read()
        
        end_time = time.time()
        duration = end_time - start_time
        speed_mbps = (len(data) / (1024 * 1024)) / duration
        
        print(f"✓ smbclient completed:")
        print(f"  Size: {len(data) / (1024*1024):.2f} MB")
        print(f"  Time: {duration:.2f} seconds")
        print(f"  Speed: {speed_mbps:.2f} MB/s")
        
        return data, duration
        
    except ImportError:
        print("❌ smbclient not installed. Install with: pip install smbclient")
        print("   Also requires: apt-get install libsmbclient-dev (Linux) or samba client tools")
        return None, None
    except Exception as e:
        print(f"❌ smbclient failed: {e}")
        return None, None


def main():
    """
    Main function to run performance tests
    """
    print("SMB File Transfer Performance Test")
    print("=" * 50)
    
    # Configuration - UPDATE THESE VALUES
    SERVER = "your-server-ip-or-name"
    SHARE = "your-share-name"
    FILEPATH = "path/to/your/file.txt"  # Path within the share
    USERNAME = "your-username"
    PASSWORD = "your-password"
    DOMAIN = ""  # Optional domain, leave empty if not needed
    
    print(f"Server: {SERVER}")
    print(f"Share: {SHARE}")
    print(f"File: {FILEPATH}")
    print(f"User: {USERNAME}")
    print("-" * 50)
    
    # Store results for comparison
    results = {}
    
    # Test all available libraries
    libraries = [
        ("smbclient", test_smbclient),
        ("smbprotocol", test_smbprotocol),
        ("pysmb", test_pysmb)
    ]
    
    for lib_name, test_func in libraries:
        print(f"\n🔍 Testing {lib_name}...")
        data, duration = test_func(SERVER, SHARE, FILEPATH, USERNAME, PASSWORD, DOMAIN)
        
        if data is not None and duration is not None:
            results[lib_name] = {
                'size': len(data),
                'duration': duration,
                'speed': (len(data) / (1024 * 1024)) / duration
            }
    
    # Summary
    if results:
        print("\n" + "=" * 50)
        print("PERFORMANCE SUMMARY")
        print("=" * 50)
        
        # Sort by speed (fastest first)
        sorted_results = sorted(results.items(), key=lambda x: x[1]['duration'])
        
        for lib_name, result in sorted_results:
            print(f"{lib_name:12}: {result['duration']:6.2f}s - {result['speed']:6.2f} MB/s")
        
        # Compare to your Spring Integration baseline
        spring_time = 5 * 60  # 5 minutes in seconds
        best_time = sorted_results[0][1]['duration']
        improvement = spring_time / best_time
        
        print(f"\nSpring Integration: {spring_time:6.2f}s (baseline)")
        print(f"Best Python result: {best_time:6.2f}s")
        print(f"Improvement factor: {improvement:.1f}x faster")
    else:
        print("\n❌ No successful tests. Check your configuration and library installations.")


if __name__ == "__main__":
    # Quick configuration check
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print(__doc__)
        sys.exit(0)
    
    print("Before running, please update the configuration variables in main():")
    print("- SERVER, SHARE, FILEPATH, USERNAME, PASSWORD")
    print("\nRequired packages:")
    print("  pip install smbprotocol pysmb")
    print("\nOptional (fastest performance):")
    print("  pip install smbclient")
    print("\nRun with --help for more information")
    print("\nContinuing with test...\n")
    
    main()