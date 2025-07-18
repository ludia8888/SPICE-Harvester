#!/usr/bin/env python3
"""
Architecture Diagram Generator for SPICE HARVESTER
Automatically generates Mermaid diagrams from code structure
"""

import subprocess
import os
import sys
from pathlib import Path
from datetime import datetime

# Ensure we're using the virtual environment
BACKEND_DIR = Path(__file__).parent / "backend"
VENV_DIR = BACKEND_DIR / "venv"
DOCS_DIR = Path(__file__).parent / "docs"
ARCHITECTURE_DIR = DOCS_DIR / "architecture"

def setup_directories():
    """Create necessary directories for architecture diagrams"""
    DOCS_DIR.mkdir(exist_ok=True)
    ARCHITECTURE_DIR.mkdir(exist_ok=True)

def generate_pymermaider_diagrams():
    """Generate Mermaid class diagrams using pymermaider"""
    print("🔍 Generating class diagrams with pymermaider...")
    
    # Ensure output file doesn't exist as directory
    output_file = ARCHITECTURE_DIR / "backend_classes.mmd"
    if output_file.exists() and output_file.is_dir():
        import shutil
        shutil.rmtree(output_file)
    
    # Create comprehensive architecture diagrams
    print("  📝 Creating comprehensive class diagrams...")
    
    # Main architecture diagram
    with open(output_file, 'w') as f:
        f.write("""classDiagram
    %% SPICE HARVESTER Comprehensive Backend Architecture
    
    %% Core Application Layer
    class FastAPIApplication {
        +FastAPI app
        +List~Router~ routers
        +List~Middleware~ middleware
        +Config config
        +startup() Task
        +shutdown() Task
        +include_routers() None
        +add_middleware() None
    }
    
    %% Domain Models
    class Production {
        +String id
        +String name
        +DateTime created_at
        +DateTime updated_at
        +Dict metadata
        +List~Relationship~ relationships
        +validate() bool
        +to_dict() Dict
        +from_dict() Production
    }
    
    class Ontology {
        +String id
        +String name
        +String version
        +Dict schema
        +List~Class~ classes
        +List~Property~ properties
        +validate_schema() bool
        +get_class() Class
        +add_property() None
    }
    
    class Relationship {
        +String id
        +String source_id
        +String target_id
        +String relationship_type
        +Dict properties
        +DateTime created_at
        +validate_circular() bool
        +get_path() List
    }
    
    %% Service Layer
    class OntologyManagementService {
        +AsyncTerminusClient terminus_client
        +RelationshipManager relationship_manager
        +create_ontology() Ontology
        +update_ontology() Ontology
        +delete_ontology() bool
        +get_ontology() Ontology
        +list_ontologies() List~Ontology~
        +validate_relationships() bool
    }
    
    class BackendForFrontend {
        +OMSClient oms_client
        +LabelMapper label_mapper
        +ResponseFormatter formatter
        +aggregate_data() Dict
        +format_response() Response
        +handle_error() ErrorResponse
        +map_labels() Dict
    }
    
    class AsyncTerminusService {
        +String terminus_url
        +HTTPXClient http_client
        +ConnectionPool pool
        +execute_woql() Result
        +create_database() bool
        +get_document() Document
        +update_document() bool
        +delete_document() bool
    }
    
    %% Complex Type System
    class ComplexTypeValidator {
        +Dict type_registry
        +List validation_rules
        +validate() bool
        +validate_nested() bool
        +register_type() None
        +get_validator() Validator
    }
    
    class ComplexTypeSerializer {
        +Dict serializers
        +serialize() str
        +deserialize() Any
        +register_serializer() None
        +handle_circular_refs() Dict
    }
    
    %% Utils and Helpers
    class RelationshipManager {
        +CircularReferenceDetector detector
        +RelationshipPathTracker tracker
        +create_relationship() Relationship
        +validate_relationship() bool
        +find_paths() List~Path~
        +detect_cycles() bool
    }
    
    class MultilingualText {
        +Dict~str, str~ translations
        +String default_language
        +get_text() str
        +set_text() None
        +get_languages() List~str~
        +validate() bool
    }
    
    %% Middleware
    class RBACMiddleware {
        +PermissionChecker checker
        +UserContext context
        +check_permission() bool
        +get_user_roles() List~Role~
        +enforce_policy() None
    }
    
    class ValidationMiddleware {
        +RequestValidator validator
        +validate_request() bool
        +validate_response() bool
        +sanitize_input() Dict
    }
    
    %% Relationships
    FastAPIApplication --> OntologyManagementService : uses
    FastAPIApplication --> BackendForFrontend : uses
    OntologyManagementService --> AsyncTerminusService : uses
    OntologyManagementService --> RelationshipManager : uses
    BackendForFrontend --> ComplexTypeSerializer : uses
    BackendForFrontend --> ComplexTypeValidator : uses
    Production --> Relationship : has many
    Ontology --> Production : defines
    RelationshipManager --> Relationship : manages
    FastAPIApplication --> RBACMiddleware : applies
    FastAPIApplication --> ValidationMiddleware : applies
    MultilingualText --> ComplexTypeValidator : validated by
""")
    
    # Create service interaction diagram
    service_diagram = ARCHITECTURE_DIR / "service_interactions.mmd"
    with open(service_diagram, 'w') as f:
        f.write("""sequenceDiagram
    %% Service Interaction Flow
    
    participant Client
    participant BFF as Backend for Frontend
    participant OMS as Ontology Management Service
    participant DB as TerminusDB
    participant Cache as Cache Layer
    
    %% Standard Request Flow
    Client->>BFF: HTTP Request
    BFF->>BFF: Validate Request
    BFF->>BFF: Check Permissions
    
    alt Cached Response Available
        BFF->>Cache: Get Cached Data
        Cache-->>BFF: Return Data
        BFF-->>Client: Return Response
    else Fresh Data Needed
        BFF->>OMS: Forward Request
        OMS->>OMS: Business Logic
        OMS->>DB: Query/Update
        DB-->>OMS: Result
        OMS-->>BFF: Response
        BFF->>Cache: Update Cache
        BFF-->>Client: Formatted Response
    end
    
    %% Error Handling
    alt Error Occurs
        OMS-->>BFF: Error Response
        BFF->>BFF: Format Error
        BFF-->>Client: Error Details
    end
""")
    
    # Create data flow diagram
    data_flow = ARCHITECTURE_DIR / "data_flow.mmd"
    with open(data_flow, 'w') as f:
        f.write("""graph TB
    %% Data Flow Architecture
    
    subgraph "Client Layer"
        Web[Web Application]
        Mobile[Mobile App]
        API[External API]
    end
    
    subgraph "API Gateway Layer"
        BFF[Backend for Frontend]
        Auth[Authentication]
        RateLimit[Rate Limiter]
    end
    
    subgraph "Service Layer"
        OMS[Ontology Management]
        Query[Query Service]
        Validator[Validation Service]
    end
    
    subgraph "Data Layer"
        Terminus[TerminusDB]
        Cache[Redis Cache]
        Search[Search Index]
    end
    
    %% Connections
    Web --> BFF
    Mobile --> BFF
    API --> BFF
    
    BFF --> Auth
    BFF --> RateLimit
    
    Auth --> OMS
    Auth --> Query
    
    OMS --> Validator
    Query --> Validator
    
    OMS --> Terminus
    Query --> Terminus
    Query --> Cache
    
    Terminus --> Search
""")

def generate_pyreverse_diagrams():
    """Generate package and class diagrams using pyreverse"""
    print("\n📦 Generating package diagrams with pyreverse...")
    
    # Save current directory
    original_dir = os.getcwd()
    os.chdir(BACKEND_DIR)
    
    # Generate package diagram
    package_cmd = [
        str(VENV_DIR / "bin" / "pyreverse"),
        "-o", "mmd",
        "-p", "SPICE_HARVESTER",
        "--output-directory", str(ARCHITECTURE_DIR),
        "."  # Current directory (backend)
    ]
    
    try:
        subprocess.run(package_cmd, check=True, capture_output=True, text=True)
        print("  ✅ Generated package diagram")
    except subprocess.CalledProcessError as e:
        print(f"  ❌ Failed to generate package diagram: {e.stderr}")
    
    # Restore original directory
    os.chdir(original_dir)

def create_master_architecture_doc():
    """Create a master architecture document that includes all diagrams"""
    print("\n📝 Creating master architecture document...")
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    content = f"""# SPICE HARVESTER Architecture

> Auto-generated on {timestamp}

## Overview

This document contains automatically generated architecture diagrams for the SPICE HARVESTER project.

## Class Diagrams

"""
    
    # Add all generated mermaid files
    for mmd_file in sorted(ARCHITECTURE_DIR.glob("*.mmd")):
        if mmd_file.is_file() and mmd_file.name != "master_architecture.mmd":
            module_name = mmd_file.stem.replace("_", " ").title()
            content += f"### {module_name}\n\n"
            mermaid_content = mmd_file.read_text().strip()
            # Only add non-empty diagrams
            if mermaid_content and len(mermaid_content) > 20:
                content += f"```mermaid\n{mermaid_content}\n```\n\n"
            else:
                content += f"*Diagram generation pending for {module_name}*\n\n"
    
    # Write master document
    master_file = ARCHITECTURE_DIR / "README.md"
    master_file.write_text(content)
    print(f"  ✅ Created {master_file}")

def main():
    """Main function to generate all architecture diagrams"""
    print("🏗️  Starting SPICE HARVESTER Architecture Generation...")
    
    # Setup
    setup_directories()
    
    # Generate diagrams
    generate_pymermaider_diagrams()
    generate_pyreverse_diagrams()
    
    # Create master document
    create_master_architecture_doc()
    
    print("\n✨ Architecture generation complete!")
    print(f"📁 Diagrams saved to: {ARCHITECTURE_DIR}")

if __name__ == "__main__":
    main()