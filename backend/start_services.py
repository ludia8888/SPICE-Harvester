#!/usr/bin/env python3
"""
🔥 THINK ULTRA! SPICE HARVESTER 서비스 시작 스크립트
모든 마이크로서비스 (OMS, BFF, Funnel)를 시작합니다.
"""

import subprocess
import time
import os
import signal
import argparse
import requests
import sys
import urllib3
from pathlib import Path

from shared.config.settings import get_settings

# Disable SSL warnings for development
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def start_service(name, path, command, port, *, protocol: str, verify_ssl: bool, health_path="/health"):
    """Start a service and verify it's running"""
    print(f"\n🚀 Starting {name}...")
    
    # Change to service directory
    original_dir = os.getcwd()
    os.chdir(path)
    
    # Start the service
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid
    )
    
    os.chdir(original_dir)
    
    # Wait for service to start
    max_attempts = 30
    for i in range(max_attempts):
        try:
            # Use verify=False for self-signed certificates in development
            response = requests.get(
                f"{protocol}://localhost:{port}{health_path}",
                verify=verify_ssl,
            )
            if response.status_code == 200:
                print(f"✅ {name} started successfully on port {port} ({protocol.upper()})")
                return process
        except (requests.ConnectionError, requests.Timeout):
            # 서비스가 아직 시작 중일 수 있음 - 정상적인 상황
            pass
        except Exception as e:
            # 다른 예외는 로그를 남김
            print(f"⚠️ {name} health check error: {type(e).__name__}: {e}")
        
        # Check if process is still running
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            print(f"❌ {name} failed to start!")
            print(f"STDOUT: {stdout.decode()}")
            print(f"STDERR: {stderr.decode()}")
            return None
            
        time.sleep(1)
        
    print(f"❌ {name} failed to respond after {max_attempts} seconds")
    return None

def stop_services(processes):
    """Stop all services"""
    print("\n🔄 Stopping services...")
    for name, process in processes.items():
        if process:
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                print(f"✅ Stopped {name}")
            except ProcessLookupError:
                # 프로세스가 이미 종료됨 - 정상
                pass
            except Exception as e:
                print(f"⚠️ Error stopping {name}: {type(e).__name__}: {e}")

def main():
    processes = {}

    parser = argparse.ArgumentParser(
        description="Start SPICE HARVESTER services locally (OMS/BFF/Funnel/Agent)."
    )
    parser.add_argument(
        "--env",
        choices=["development", "production"],
        default=get_settings().environment.value,
        help="Environment preset (default: development)",
    )
    args = parser.parse_args()

    # Ensure local-mode defaults unless explicitly overridden.
    if args.env == "development":
        os.environ.setdefault("DOCKER_CONTAINER", "false")
    
    # Service paths
    base_path = Path(__file__).resolve().parent
    oms_path = str(base_path / "oms")
    bff_path = str(base_path / "bff")
    funnel_path = str(base_path / "funnel")
    agent_path = str(base_path / "agent")
    
    # Verify all main.py files exist
    services_config = [
        ("OMS", oms_path, 8000),
        ("BFF", bff_path, 8002),
        ("Funnel", funnel_path, 8003),
        ("Agent", agent_path, 8004),
    ]
    
    for name, path, port in services_config:
        if not os.path.exists(os.path.join(path, "main.py")):
            print(f"❌ {name} main.py not found at {path}")
            return 1
    
    settings = get_settings()

    # Determine protocol
    use_https = bool(settings.services.use_https)
    protocol = "https" if use_https else "http"
    verify_ssl = False if use_https else True
    
    if use_https:
        print("\n🔐 Starting services with HTTPS enabled")
        print("⚠️  Using self-signed certificates - expect browser warnings")
    else:
        print("\n🔓 Starting services with HTTP (no encryption)")
    
    try:
        # Start OMS service first (core ontology management)
        oms_process = start_service(
            "OMS",
            oms_path,
            f"{sys.executable} -m uvicorn main:app --host 0.0.0.0 --port 8000",
            8000,
            protocol=protocol,
            verify_ssl=verify_ssl,
        )
        
        if not oms_process:
            return 1
            
        processes["OMS"] = oms_process
        
        # Start Funnel service (type inference)
        funnel_process = start_service(
            "Funnel",
            funnel_path,
            f"{sys.executable} -m uvicorn main:app --host 0.0.0.0 --port 8003",
            8003,
            protocol=protocol,
            verify_ssl=verify_ssl,
        )
        
        if not funnel_process:
            stop_services(processes)
            return 1
            
        processes["Funnel"] = funnel_process
        
        # Start BFF (depends on OMS and Funnel)
        bff_process = start_service(
            "BFF", 
            bff_path,
            f"{sys.executable} -m uvicorn main:app --host 0.0.0.0 --port 8002",
            8002,
            protocol=protocol,
            verify_ssl=verify_ssl,
            health_path="/api/v1/health",
        )
        
        if not bff_process:
            stop_services(processes)
            return 1
            
        processes["BFF"] = bff_process

        # Start Agent service (depends on core APIs)
        agent_process = start_service(
            "Agent",
            agent_path,
            f"{sys.executable} -m uvicorn main:app --host 0.0.0.0 --port 8004",
            8004,
            protocol=protocol,
            verify_ssl=verify_ssl,
            health_path="/health",
        )

        if not agent_process:
            stop_services(processes)
            return 1

        processes["Agent"] = agent_process
        
        print("\n🎉 All services started successfully!")
        print("\n📋 Services running:")
        print(f"  - OMS (Ontology Management): {protocol}://localhost:8000")
        print(f"  - BFF (Backend for Frontend): {protocol}://localhost:8002")
        print(f"  - Funnel (Type Inference): {protocol}://localhost:8003")
        print(f"  - Agent (tool runner): {protocol}://localhost:8004")
        print("\n🔍 API Documentation:")
        print(f"  - OMS Docs: {protocol}://localhost:8000/docs")
        print(f"  - BFF Docs: {protocol}://localhost:8002/docs")
        print(f"  - Funnel Docs: {protocol}://localhost:8003/docs")
        print(f"  - Agent Docs: {protocol}://localhost:8004/docs")
        print("\n💡 Key APIs:")
        print("  - Database Management: POST /api/v1/databases")
        print("  - Ontology Creation: POST /api/v1/databases/{db_name}/ontology")
        print("  - Schema Suggestion: POST /api/v1/databases/{db_name}/suggest-schema-from-data")
        print("\nPress Ctrl+C to stop all services...")
        
        # Keep running until interrupted
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    finally:
        stop_services(processes)
        
    return 0

if __name__ == "__main__":
    sys.exit(main())
