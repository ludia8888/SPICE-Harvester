"""
BFF Adapter Service

This service acts as an adapter between the user-friendly BFF layer and the 
core OMS (Ontology Management Service), handling:

1. Label-to-ID conversion
2. Request/response transformation
3. Error handling and user-friendly messages
4. OMS API delegation

This replaces the duplicated business logic that was previously in BFF routers.
"""

import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from shared.models.ontology import OntologyResponse
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input, validate_db_name, validate_class_id
from shared.utils.label_mapper import LabelMapper
from shared.utils.id_generator import generate_simple_id

if TYPE_CHECKING:
    from bff.dependencies import TerminusService

logger = logging.getLogger(__name__)


class BFFAdapterService:
    """
    Adapter service that bridges the user-friendly BFF layer with the core OMS layer.
    
    This service eliminates code duplication by:
    - Centralizing label-to-ID conversion logic
    - Delegating all business logic to OMS
    - Providing consistent error handling
    - Transforming responses for user-friendly consumption
    """
    
    def __init__(self, terminus_service: "TerminusService", label_mapper: LabelMapper):
        self.terminus_service = terminus_service
        self.label_mapper = label_mapper
    
    async def create_ontology(
        self,
        db_name: str,
        ontology_data: Dict[str, Any],
        language: str = "ko"
    ) -> OntologyResponse:
        """
        Create ontology through OMS with label-to-ID conversion.
        
        This method replaces the duplicated ontology creation logic
        that was present in both BFF and OMS routers.
        """
        try:
            # 1. Input validation and sanitization
            db_name = validate_db_name(db_name)
            ontology_dict = sanitize_input(ontology_data.copy())
            
            # 2. ID generation or validation
            if ontology_dict.get('id'):
                class_id = validate_class_id(ontology_dict['id'])
                logger.info(f"Using provided class_id '{class_id}'")
            else:
                class_id = generate_simple_id(
                    label=ontology_dict.get('label', ''),
                    use_timestamp_for_korean=True,
                    default_fallback="UnnamedClass"
                )
                logger.info(f"Generated class_id '{class_id}' from label")
            
            ontology_dict['id'] = class_id
            
            # 3. Register label mappings (before OMS call for consistency)
            await self._register_label_mappings(db_name, ontology_dict)
            
            # 4. Delegate to OMS (core business logic)
            oms_result = await self.terminus_service.create_ontology(db_name, ontology_dict)
            
            # 5. Transform response for user consumption
            return self._transform_ontology_response(
                oms_result, 
                f"'{ontology_dict.get('label', class_id)}' 온톨로지가 생성되었습니다",
                ontology_dict
            )
            
        except Exception as e:
            logger.error(f"Failed to create ontology: {e}")
            raise
    
    async def create_advanced_ontology(
        self,
        db_name: str,
        ontology_data: Dict[str, Any],
        language: str = "ko"
    ) -> OntologyResponse:
        """
        Create ontology with advanced relationship validation through OMS.
        
        This eliminates the duplicate advanced creation logic between BFF and OMS.
        """
        try:
            # 1. Basic validation and ID generation (same as create_ontology)
            db_name = validate_db_name(db_name)
            ontology_dict = sanitize_input(ontology_data.copy())
            
            if not ontology_dict.get('id'):
                class_id = generate_simple_id(
                    label=ontology_dict.get('label', ''),
                    use_timestamp_for_korean=True,
                    default_fallback="UnnamedClass"
                )
                ontology_dict['id'] = class_id
            
            # 2. Register label mappings
            await self._register_label_mappings(db_name, ontology_dict)
            
            # 3. Delegate to OMS advanced creation (no duplication of validation logic)
            oms_result = await self.terminus_service.create_ontology_with_advanced_relationships(
                db_name, ontology_dict
            )
            
            # 4. Transform response
            return self._transform_ontology_response(
                oms_result,
                f"고급 관계를 포함한 '{ontology_dict.get('label', ontology_dict['id'])}' 온톨로지가 생성되었습니다",
                ontology_dict
            )
            
        except Exception as e:
            logger.error(f"Failed to create advanced ontology: {e}")
            raise
    
    async def validate_relationships(
        self,
        db_name: str,
        validation_data: Dict[str, Any],
        language: str = "ko"
    ) -> Dict[str, Any]:
        """
        Validate relationships through OMS.
        
        This eliminates duplicate validation logic between BFF and OMS.
        """
        try:
            # 1. Basic validation
            db_name = validate_db_name(db_name)
            sanitized_data = sanitize_input(validation_data)
            
            # 2. Convert labels to IDs if needed
            converted_data = await self._convert_labels_to_ids(db_name, sanitized_data, language)
            
            # 3. Delegate to OMS
            oms_result = await self.terminus_service.validate_relationships(db_name, converted_data)
            
            # 4. Convert response back to labels for user consumption
            return await self._convert_ids_to_labels(db_name, oms_result, language)
            
        except Exception as e:
            logger.error(f"Failed to validate relationships: {e}")
            raise
    
    async def detect_circular_references(
        self,
        db_name: str,
        detection_data: Dict[str, Any],
        language: str = "ko"
    ) -> Dict[str, Any]:
        """
        Detect circular references through OMS.
        
        This eliminates duplicate detection logic between BFF and OMS.
        """
        try:
            # 1. Basic validation and conversion
            db_name = validate_db_name(db_name)
            sanitized_data = sanitize_input(detection_data)
            converted_data = await self._convert_labels_to_ids(db_name, sanitized_data, language)
            
            # 2. Delegate to OMS
            oms_result = await self.terminus_service.detect_circular_references(db_name, converted_data)
            
            # 3. Convert response back to labels
            return await self._convert_ids_to_labels(db_name, oms_result, language)
            
        except Exception as e:
            logger.error(f"Failed to detect circular references: {e}")
            raise
    
    async def find_relationship_paths(
        self,
        db_name: str,
        start_entity_label: str,
        target_entity_label: Optional[str] = None,
        max_depth: int = 3,
        path_type: str = "shortest",
        language: str = "ko"
    ) -> Dict[str, Any]:
        """
        Find relationship paths through OMS.
        
        This eliminates duplicate path-finding logic between BFF and OMS.
        """
        try:
            # 1. Basic validation
            db_name = validate_db_name(db_name)
            
            # 2. Convert labels to IDs
            start_entity_id = await self.label_mapper.get_class_id(db_name, start_entity_label, language)
            target_entity_id = None
            if target_entity_label:
                target_entity_id = await self.label_mapper.get_class_id(db_name, target_entity_label, language)
            
            # 3. Delegate to OMS
            oms_result = await self.terminus_service.find_relationship_paths(
                db_name, start_entity_id, target_entity_id, max_depth, path_type
            )
            
            # 4. Convert response back to labels
            return await self._convert_ids_to_labels(db_name, oms_result, language)
            
        except Exception as e:
            logger.error(f"Failed to find relationship paths: {e}")
            raise
    
    async def _register_label_mappings(self, db_name: str, ontology_dict: Dict[str, Any]) -> None:
        """Register class and property label mappings"""
        class_id = ontology_dict.get('id')
        
        # Register class mapping
        if class_id:
            await self.label_mapper.register_class(
                db_name=db_name,
                class_id=class_id,
                label=ontology_dict.get('label'),
                description=ontology_dict.get('description')
            )
        
        # Register property mappings
        for prop in ontology_dict.get('properties', []):
            if isinstance(prop, dict):
                await self.label_mapper.register_property(
                    db_name=db_name,
                    class_id=class_id,
                    property_id=prop.get('name'),
                    label=prop.get('label')
                )
        
        # Register relationship mappings
        for rel in ontology_dict.get('relationships', []):
            if isinstance(rel, dict):
                await self.label_mapper.register_relationship(
                    db_name=db_name,
                    predicate=rel.get('predicate'),
                    label=rel.get('label')
                )
    
    async def _convert_labels_to_ids(
        self, 
        db_name: str, 
        data: Dict[str, Any], 
        language: str
    ) -> Dict[str, Any]:
        """Convert labels in request data to internal IDs"""
        # Implementation would depend on the specific data structure
        # This is a placeholder for the label-to-ID conversion logic
        converted_data = data.copy()
        
        # Convert class labels to IDs
        if 'class_label' in converted_data:
            class_id = await self.label_mapper.get_class_id(
                db_name, converted_data['class_label'], language
            )
            converted_data['class_id'] = class_id
            del converted_data['class_label']
        
        return converted_data
    
    async def _convert_ids_to_labels(
        self, 
        db_name: str, 
        data: Dict[str, Any], 
        language: str
    ) -> Dict[str, Any]:
        """Convert internal IDs in response data to user-friendly labels"""
        # Implementation would depend on the specific data structure
        # This is a placeholder for the ID-to-label conversion logic
        converted_data = data.copy()
        
        # Convert class IDs to labels
        if 'class_id' in converted_data:
            class_label = await self.label_mapper.get_class_label(
                db_name, converted_data['class_id'], language
            )
            converted_data['class_label'] = class_label
        
        return converted_data
    
    def _transform_ontology_response(
        self,
        oms_result: Dict[str, Any],
        success_message: str,
        ontology_dict: Dict[str, Any]
    ) -> OntologyResponse:
        """Transform OMS response into user-friendly OntologyResponse"""
        
        # Extract created class ID from OMS response
        oms_data = oms_result.get("data", [])
        if isinstance(oms_data, list) and len(oms_data) > 0:
            created_class_id = oms_data[0]
        else:
            created_class_id = ontology_dict.get("id")
        
        # Return OntologyResponse directly with all required fields
        return OntologyResponse(
            id=created_class_id,
            label=ontology_dict.get('label', ''),
            description=ontology_dict.get('description'),
            parent_class=ontology_dict.get('parent_class'),
            abstract=ontology_dict.get('abstract', False),
            properties=ontology_dict.get('properties', []),
            relationships=ontology_dict.get('relationships', []),
            metadata={
                "created": True, 
                "source": "bff_adapter",
                "message": success_message
            }
        )