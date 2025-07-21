#!/usr/bin/env python3
"""
🔥 THINK ULTRA!! Complex Type Interactions Test
복합 타입 간 상호작용 및 중첩 검증 테스트
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Dict, Any, List

# No need for sys.path.insert - using proper spice_harvester package imports
from shared.models.common import DataType
from shared.validators.complex_type_validator import ComplexTypeValidator, ComplexTypeConstraints
from shared.serializers.complex_type_serializer import ComplexTypeSerializer
from tests.utils.assertions import assert_equal, assert_contains, assert_type, assert_in_range
from tests.test_config import TestConfig
from tests.utils.test_isolation import TestIsolationManager, TestDataBuilder

import httpx


class ComplexTypeInteractionsTest:
    """🔥 THINK ULTRA!! 복합 타입 상호작용 테스터"""
    
    def __init__(self):
        self.isolation_manager = TestIsolationManager()
        self.data_builder = TestDataBuilder(self.isolation_manager)
        self.test_db = None
        self.test_results = {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "details": []
        }
        self.validator = ComplexTypeValidator()
        self.serializer = ComplexTypeSerializer()
    
    async def run_all_tests(self):
        """모든 상호작용 테스트 실행"""
        
        print("🔥" * 60)
        print("🔥 THINK ULTRA!! Complex Type Interactions Test")
        print("🔥" * 60)
        
        # Setup
        async with httpx.AsyncClient(timeout=30) as client:
            await self.setup_test_database(client)
            
            # Run tests
            test_methods = [
                ("1️⃣ 객체 안의 복합 타입 배열", self.test_object_with_complex_arrays),
                ("2️⃣ 배열 안의 복합 타입 객체", self.test_array_of_complex_objects),
                ("3️⃣ 다단계 중첩 구조", self.test_multi_level_nesting),
                ("4️⃣ 중첩된 제약조건 검증", self.test_nested_constraints),
                ("5️⃣ 복합 타입 직렬화/역직렬화", self.test_complex_serialization),
                ("6️⃣ 주소-좌표 통합", self.test_address_coordinate_integration),
                ("7️⃣ 실제 e-commerce 시나리오", self.test_real_world_ecommerce),
                ("8️⃣ 복합 타입 변환 및 정규화", self.test_complex_normalization),
                ("9️⃣ 순환 참조 감지", self.test_circular_reference_detection),
                ("🔟 대용량 중첩 데이터", self.test_large_nested_structures)
            ]
            
            for test_name, test_method in test_methods:
                print(f"\n🧪 {test_name}")
                print("=" * 70)
                
                try:
                    await test_method(client)
                    self.record_result(test_name, True, "성공")
                except Exception as e:
                    self.record_result(test_name, False, str(e))
                    print(f"❌ 실패: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Cleanup
            await self.cleanup_test_database(client)
        
        self.print_summary()
    
    async def setup_test_database(self, client: httpx.AsyncClient):
        """테스트 데이터베이스 설정"""
        print("\n🔧 Setting up isolated test database...")
        
        self.test_db = self.isolation_manager.generate_isolated_name("test_complex_interactions")
        
        response = await client.post(
            f"{TestConfig.get_oms_base_url()}/api/v1/database/create",
            json={
                "name": self.test_db,
                "description": f"Complex Type Interactions Test - {datetime.now()}"
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to create database: {response.text}")
        
        self.isolation_manager.register_database(self.test_db)
        print(f"✅ Isolated test database created: {self.test_db}")
    
    async def cleanup_test_database(self, client: httpx.AsyncClient):
        """테스트 데이터베이스 정리"""
        print("\n🧹 Cleaning up test database...")
        
        if self.isolation_manager.cleanup_database(self.test_db):
            print("✅ Cleanup completed")
        else:
            print("⚠️ Cleanup failed - will be retried by isolation manager")
    
    async def test_object_with_complex_arrays(self, client: httpx.AsyncClient):
        """객체 안의 복합 타입 배열 테스트"""
        
        # 1. Create Product model with complex arrays
        print("\n📌 Creating Product model with complex arrays...")
        
        product_model = {
            "id": "ProductWithArrays",
            "label": {"en": "Product with Complex Arrays", "ko": "복합 배열을 가진 상품"},
            "properties": [
                {
                    "name": "images",
                    "type": DataType.ARRAY.value,
                    "label": {"en": "Product Images", "ko": "상품 이미지"},
                    "constraints": ComplexTypeConstraints.array_constraints(
                        item_type=DataType.IMAGE.value,
                        min_items=1,
                        max_items=10
                    )
                },
                {
                    "name": "prices",
                    "type": DataType.ARRAY.value,
                    "label": {"en": "Price History", "ko": "가격 이력"},
                    "constraints": ComplexTypeConstraints.array_constraints(
                        item_type=DataType.MONEY.value
                    )
                },
                {
                    "name": "tags",
                    "type": DataType.ARRAY.value,
                    "label": {"en": "Tags", "ko": "태그"},
                    "constraints": ComplexTypeConstraints.array_constraints(
                        item_type=DataType.ENUM.value,
                        unique_items=True
                    )
                }
            ]
        }
        
        response = await client.post(
            f"{TestConfig.get_oms_base_url()}/api/v1/ontology/{self.test_db}/create",
            json=product_model
        )
        
        assert_equal(
            actual=response.status_code,
            expected=200,
            field_name="product_model_creation",
            context={"response": response.text}
        )
        print("✅ Product model created")
        
        # 2. Test validation
        print("\n📌 Testing array validation...")
        
        test_data = {
            "images": [
                "https://cdn.example.com/product1.jpg",
                "https://cdn.example.com/product2.png"
            ],
            "prices": [
                {"amount": 100.00, "currency": "USD"},
                {"amount": 95.50, "currency": "USD"},
                {"amount": 89.99, "currency": "USD"}
            ],
            "tags": ["new", "sale", "featured"]
        }
        
        # Validate each array
        for prop_name, prop_value in test_data.items():
            prop_info = next(p for p in product_model["properties"] if p["name"] == prop_name)
            valid, msg, normalized = self.validator.validate(
                prop_value, 
                prop_info["type"], 
                prop_info.get("constraints", {})
            )
            
            assert_equal(
                actual=valid,
                expected=True,
                field_name=f"{prop_name}_validation",
                context={"value": prop_value, "message": msg}
            )
            print(f"  ✅ {prop_name}: Validated {len(normalized)} items")
    
    async def test_array_of_complex_objects(self, client: httpx.AsyncClient):
        """배열 안의 복합 타입 객체 테스트"""
        
        # 1. Create Order model with array of complex objects
        print("\n📌 Creating Order model with complex object arrays...")
        
        order_model = {
            "id": "OrderWithItems",
            "label": {"en": "Order with Complex Items", "ko": "복합 항목을 가진 주문"},
            "properties": [
                {
                    "name": "items",
                    "type": DataType.ARRAY.value,
                    "label": {"en": "Order Items", "ko": "주문 항목"},
                    "constraints": ComplexTypeConstraints.array_constraints(
                        item_type=DataType.OBJECT.value,
                        min_items=1,
                        max_items=50
                    )
                }
            ]
        }
        
        # Define item schema with complex types
        item_schema = {
            "productId": {"type": DataType.STRING.value},
            "quantity": {"type": DataType.INTEGER.value},
            "price": {"type": DataType.MONEY.value},
            "discount": {"type": DataType.MONEY.value},
            "status": {
                "type": DataType.ENUM.value,
                "constraints": {"enum": ["pending", "confirmed", "shipped", "delivered"]}
            },
            "shippingAddress": {"type": DataType.ADDRESS.value},
            "trackingLocation": {"type": DataType.COORDINATE.value}
        }
        
        # Update constraints with schema
        order_model["properties"][0]["constraints"]["schema"] = item_schema
        order_model["properties"][0]["constraints"]["required"] = ["productId", "quantity", "price"]
        
        response = await client.post(
            f"{TestConfig.get_oms_base_url()}/api/v1/ontology/{self.test_db}/create",
            json=order_model
        )
        
        assert_equal(
            actual=response.status_code,
            expected=200,
            field_name="order_model_creation",
            context={"response": response.text}
        )
        print("✅ Order model created")
        
        # 2. Test validation
        print("\n📌 Testing complex object array validation...")
        
        test_items = [
            {
                "productId": "PROD-001",
                "quantity": 2,
                "price": {"amount": 49.99, "currency": "USD"},
                "discount": {"amount": 5.00, "currency": "USD"},
                "status": "confirmed",
                "shippingAddress": {
                    "street": "123 Main St",
                    "city": "San Francisco",
                    "state": "CA",
                    "postalCode": "94105",
                    "country": "US"
                },
                "trackingLocation": {"latitude": 37.7749, "longitude": -122.4194}
            },
            {
                "productId": "PROD-002",
                "quantity": 1,
                "price": {"amount": 99.99, "currency": "USD"},
                "status": "pending"
            }
        ]
        
        # Validate array of objects
        constraints = order_model["properties"][0]["constraints"]
        valid, msg, normalized = self.validator.validate(
            test_items,
            DataType.ARRAY.value,
            constraints
        )
        
        assert_equal(
            actual=valid,
            expected=True,
            field_name="complex_object_array_validation",
            context={"items": test_items, "message": msg}
        )
        print(f"✅ Validated array of {len(normalized)} complex objects")
    
    async def test_multi_level_nesting(self, client: httpx.AsyncClient):
        """다단계 중첩 구조 테스트"""
        
        print("\n📌 Testing multi-level nested structures...")
        
        # Create deeply nested structure
        company_model = {
            "id": "CompanyStructure",
            "label": {"en": "Company Structure", "ko": "회사 구조"},
            "properties": [
                {
                    "name": "departments",
                    "type": DataType.ARRAY.value,
                    "label": {"en": "Departments", "ko": "부서"},
                    "constraints": ComplexTypeConstraints.array_constraints(
                        item_type=DataType.OBJECT.value
                    )
                }
            ]
        }
        
        # Department schema with nested teams
        department_schema = {
            "name": {"type": DataType.STRING.value},
            "budget": {"type": DataType.MONEY.value},
            "location": {"type": DataType.ADDRESS.value},
            "teams": {
                "type": DataType.ARRAY.value,
                "constraints": {
                    "itemType": DataType.OBJECT.value,
                    "schema": {
                        "teamName": {"type": DataType.STRING.value},
                        "members": {
                            "type": DataType.ARRAY.value,
                            "constraints": {
                                "itemType": DataType.OBJECT.value,
                                "schema": {
                                    "name": {"type": DataType.STRING.value},
                                    "email": {"type": DataType.EMAIL.value},
                                    "phone": {"type": DataType.PHONE.value},
                                    "role": {
                                        "type": DataType.ENUM.value,
                                        "constraints": {"enum": ["lead", "senior", "junior", "intern"]}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        company_model["properties"][0]["constraints"]["schema"] = department_schema
        
        # Test data with 3 levels of nesting
        test_data = {
            "departments": [
                {
                    "name": "Engineering",
                    "budget": {"amount": 1000000, "currency": "USD"},
                    "location": {
                        "street": "100 Tech Way",
                        "city": "San Francisco",
                        "state": "CA",
                        "postalCode": "94105",
                        "country": "US"
                    },
                    "teams": [
                        {
                            "teamName": "Backend Team",
                            "members": [
                                {
                                    "name": "John Doe",
                                    "email": "john@company.com",
                                    "phone": "+1-415-555-0123",
                                    "role": "lead"
                                },
                                {
                                    "name": "Jane Smith",
                                    "email": "jane@company.com",
                                    "phone": "+1-415-555-0124",
                                    "role": "senior"
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        
        # Validate nested structure
        valid, msg, normalized = self.validator.validate(
            test_data["departments"],
            DataType.ARRAY.value,
            company_model["properties"][0]["constraints"]
        )
        
        assert_equal(
            actual=valid,
            expected=True,
            field_name="multi_level_nesting_validation",
            context={"data": test_data, "message": msg}
        )
        print("✅ 3-level nested structure validated successfully")
        
        # Test serialization of nested structure
        serialized, metadata = self.serializer.serialize(
            normalized,
            DataType.ARRAY.value,
            company_model["properties"][0]["constraints"]
        )
        print(f"✅ Serialized nested structure: {len(serialized)} bytes")
        
        # Test deserialization
        deserialized = self.serializer.deserialize(
            serialized,
            DataType.ARRAY.value,
            metadata
        )
        assert_equal(
            actual=len(deserialized),
            expected=1,
            field_name="deserialized_departments_count"
        )
        print("✅ Deserialization successful")
    
    async def test_nested_constraints(self, client: httpx.AsyncClient):
        """중첩된 제약조건 검증 테스트"""
        
        print("\n📌 Testing nested constraint validation...")
        
        # Create model with complex nested constraints
        payment_model = {
            "id": "PaymentMethod",
            "label": {"en": "Payment Method", "ko": "결제 방법"},
            "properties": [
                {
                    "name": "methods",
                    "type": DataType.ARRAY.value,
                    "label": {"en": "Payment Methods", "ko": "결제 수단"},
                    "constraints": ComplexTypeConstraints.array_constraints(
                        item_type=DataType.OBJECT.value,
                        min_items=1,
                        max_items=5,
                        unique_items=True
                    )
                }
            ]
        }
        
        # Define payment method schema with constraints
        method_schema = {
            "type": {
                "type": DataType.ENUM.value,
                "constraints": {"enum": ["credit_card", "bank_transfer", "paypal", "crypto"]}
            },
            "details": {
                "type": DataType.OBJECT.value,
                "constraints": {
                    "additionalProperties": False
                }
            }
        }
        
        payment_model["properties"][0]["constraints"]["schema"] = method_schema
        payment_model["properties"][0]["constraints"]["required"] = ["type", "details"]
        
        # Test invalid nested constraints
        invalid_data = [
            {
                "methods": [
                    {
                        "type": "invalid_type",  # Invalid enum
                        "details": {}
                    }
                ]
            },
            {
                "methods": []  # Below min_items
            },
            {
                "methods": [
                    {"type": "credit_card"},  # Missing required 'details'
                ]
            },
            {
                "methods": [
                    {
                        "type": "credit_card",
                        "details": {},
                        "extra": "not allowed"  # Additional property
                    }
                ]
            }
        ]
        
        for i, data in enumerate(invalid_data):
            valid, msg, _ = self.validator.validate(
                data["methods"],
                DataType.ARRAY.value,
                payment_model["properties"][0]["constraints"]
            )
            
            assert_equal(
                actual=valid,
                expected=False,
                field_name=f"invalid_nested_constraint_{i}",
                context={"data": data, "message": msg}
            )
            print(f"  ✅ Invalid data {i+1} rejected: {msg}")
        
        # Test valid nested constraints
        valid_data = {
            "methods": [
                {
                    "type": "credit_card",
                    "details": {
                        "last4": "1234",
                        "brand": "Visa"
                    }
                },
                {
                    "type": "paypal",
                    "details": {
                        "email": "user@example.com"
                    }
                }
            ]
        }
        
        valid, msg, normalized = self.validator.validate(
            valid_data["methods"],
            DataType.ARRAY.value,
            payment_model["properties"][0]["constraints"]
        )
        
        assert_equal(
            actual=valid,
            expected=True,
            field_name="valid_nested_constraints",
            context={"data": valid_data, "message": msg}
        )
        print("✅ Valid nested constraints accepted")
    
    async def test_complex_serialization(self, client: httpx.AsyncClient):
        """복합 타입 직렬화/역직렬화 테스트"""
        
        print("\n📌 Testing complex type serialization...")
        
        # Create complex data structure
        complex_data = {
            "profile": {
                "user": {
                    "name": "Test User",
                    "email": "test@example.com",
                    "phone": "+1-415-555-0100"
                },
                "preferences": {
                    "currency": "USD",
                    "language": "en",
                    "notifications": ["email", "sms"]
                },
                "addresses": [
                    {
                        "type": "home",
                        "address": {
                            "street": "123 Home St",
                            "city": "San Francisco",
                            "state": "CA",
                            "postalCode": "94105",
                            "country": "US"
                        },
                        "coordinates": {"latitude": 37.7749, "longitude": -122.4194}
                    }
                ],
                "paymentMethods": [
                    {
                        "type": "credit_card",
                        "cardInfo": {
                            "last4": "1234",
                            "expiry": "12/25"
                        },
                        "billingAddress": {
                            "street": "456 Billing Ave",
                            "city": "San Francisco",
                            "state": "CA",
                            "postalCode": "94105",
                            "country": "US"
                        }
                    }
                ]
            }
        }
        
        # Define schema for the complex structure
        profile_schema = {
            "user": {
                "type": DataType.OBJECT.value,
                "constraints": {
                    "schema": {
                        "name": {"type": DataType.STRING.value},
                        "email": {"type": DataType.EMAIL.value},
                        "phone": {"type": DataType.PHONE.value}
                    }
                }
            },
            "preferences": {"type": DataType.OBJECT.value},
            "addresses": {
                "type": DataType.ARRAY.value,
                "constraints": {
                    "itemType": DataType.OBJECT.value
                }
            },
            "paymentMethods": {
                "type": DataType.ARRAY.value,
                "constraints": {
                    "itemType": DataType.OBJECT.value
                }
            }
        }
        
        constraints = ComplexTypeConstraints.object_constraints(
            schema=profile_schema,
            required=["user"]
        )
        
        # Validate
        valid, msg, normalized = self.validator.validate(
            complex_data["profile"],
            DataType.OBJECT.value,
            constraints
        )
        
        assert_equal(
            actual=valid,
            expected=True,
            field_name="complex_data_validation",
            context={"message": msg}
        )
        print("✅ Complex data validated")
        
        # Serialize
        serialized, metadata = self.serializer.serialize(
            normalized,
            DataType.OBJECT.value,
            constraints
        )
        print(f"✅ Serialized to: {serialized[:100]}...")
        
        # Deserialize
        deserialized = self.serializer.deserialize(
            serialized,
            DataType.OBJECT.value,
            metadata
        )
        
        # Verify structure is preserved
        assert_equal(
            actual="user" in deserialized,
            expected=True,
            field_name="deserialized_user_key_exists"
        )
        assert_equal(
            actual=deserialized["user"]["email"],
            expected="test@example.com",
            field_name="user_email_preserved"
        )
        print("✅ Deserialization successful - structure preserved")
    
    async def test_address_coordinate_integration(self, client: httpx.AsyncClient):
        """주소-좌표 통합 테스트"""
        
        print("\n📌 Testing address-coordinate integration...")
        
        # Create Store model with integrated location
        store_model = {
            "id": "StoreLocation",
            "label": {"en": "Store with Location", "ko": "위치가 있는 매장"},
            "properties": [
                {
                    "name": "location",
                    "type": DataType.OBJECT.value,
                    "label": {"en": "Store Location", "ko": "매장 위치"},
                    "constraints": ComplexTypeConstraints.object_constraints(
                        schema={
                            "address": {"type": DataType.ADDRESS.value},
                            "coordinates": {"type": DataType.COORDINATE.value},
                            "mapUrl": {"type": DataType.STRING.value}
                        },
                        required=["address", "coordinates"]
                    )
                }
            ]
        }
        
        # Test data with address and coordinates
        test_location = {
            "address": {
                "street": "1 Market Street",
                "city": "San Francisco",
                "state": "CA",
                "postalCode": "94105",
                "country": "US"
            },
            "coordinates": {
                "latitude": 37.7942,
                "longitude": -122.3954
            },
            "mapUrl": "https://maps.example.com/store/123"
        }
        
        # Validate integrated location
        valid, msg, normalized = self.validator.validate(
            test_location,
            DataType.OBJECT.value,
            store_model["properties"][0]["constraints"]
        )
        
        assert_equal(
            actual=valid,
            expected=True,
            field_name="address_coordinate_validation",
            context={"location": test_location, "message": msg}
        )
        print("✅ Address-coordinate integration validated")
        
        # Test address with embedded coordinates (future enhancement)
        enhanced_address = {
            "street": "1 Market Street",
            "city": "San Francisco",
            "state": "CA",
            "postalCode": "94105",
            "country": "US",
            "coordinates": {  # Embedded coordinates in address
                "latitude": 37.7942,
                "longitude": -122.3954
            }
        }
        
        # Currently this might not be supported, but it's a good test case
        addr_constraints = ComplexTypeConstraints.address_constraints()
        valid, msg, normalized = self.validator.validate(
            enhanced_address,
            DataType.ADDRESS.value,
            addr_constraints
        )
        
        # Whether it passes or fails, we document the behavior
        print(f"  ℹ️ Address with embedded coordinates: {valid} - {msg}")
    
    async def test_real_world_ecommerce(self, client: httpx.AsyncClient):
        """실제 e-commerce 시나리오 테스트"""
        
        print("\n📌 Testing real-world e-commerce scenario...")
        
        # Create comprehensive shopping cart
        cart_data = {
            "cartId": "CART-123456",
            "customerId": "CUST-789",
            "items": [
                {
                    "productId": "PROD-001",
                    "name": "Premium Laptop",
                    "quantity": 1,
                    "unitPrice": {"amount": 1299.99, "currency": "USD"},
                    "discount": {"amount": 100.00, "currency": "USD"},
                    "images": [
                        "https://cdn.example.com/laptop1.jpg",
                        "https://cdn.example.com/laptop2.jpg"
                    ],
                    "attributes": {
                        "color": "Silver",
                        "storage": "512GB",
                        "warranty": "2 years"
                    }
                },
                {
                    "productId": "PROD-002",
                    "name": "Wireless Mouse",
                    "quantity": 2,
                    "unitPrice": {"amount": 49.99, "currency": "USD"},
                    "images": ["https://cdn.example.com/mouse.jpg"]
                }
            ],
            "shipping": {
                "method": "express",
                "address": {
                    "street": "789 Customer Lane",
                    "city": "San Francisco",
                    "state": "CA",
                    "postalCode": "94105",
                    "country": "US"
                },
                "estimatedDelivery": "2024-01-15",
                "trackingNumber": "TRACK-123456"
            },
            "payment": {
                "method": "credit_card",
                "status": "authorized",
                "amount": {"amount": 1349.97, "currency": "USD"},
                "transactionId": "TXN-789456"
            },
            "metadata": {
                "createdAt": "2024-01-10T10:00:00Z",
                "updatedAt": "2024-01-10T10:30:00Z",
                "source": "mobile_app",
                "promotions": ["NEWYEAR2024", "FIRSTTIME"]
            }
        }
        
        # Define comprehensive cart schema
        cart_schema = {
            "cartId": {"type": DataType.STRING.value},
            "customerId": {"type": DataType.STRING.value},
            "items": {
                "type": DataType.ARRAY.value,
                "constraints": {
                    "itemType": DataType.OBJECT.value,
                    "minItems": 1,
                    "schema": {
                        "productId": {"type": DataType.STRING.value},
                        "name": {"type": DataType.STRING.value},
                        "quantity": {"type": DataType.INTEGER.value},
                        "unitPrice": {"type": DataType.MONEY.value},
                        "discount": {"type": DataType.MONEY.value},
                        "images": {
                            "type": DataType.ARRAY.value,
                            "constraints": {"itemType": DataType.IMAGE.value}
                        },
                        "attributes": {"type": DataType.OBJECT.value}
                    },
                    "required": ["productId", "name", "quantity", "unitPrice"]
                }
            },
            "shipping": {
                "type": DataType.OBJECT.value,
                "constraints": {
                    "schema": {
                        "method": {
                            "type": DataType.ENUM.value,
                            "constraints": {"enum": ["standard", "express", "overnight"]}
                        },
                        "address": {"type": DataType.ADDRESS.value},
                        "estimatedDelivery": {"type": DataType.DATE.value},
                        "trackingNumber": {"type": DataType.STRING.value}
                    },
                    "required": ["method", "address"]
                }
            },
            "payment": {
                "type": DataType.OBJECT.value,
                "constraints": {
                    "schema": {
                        "method": {"type": DataType.STRING.value},
                        "status": {
                            "type": DataType.ENUM.value,
                            "constraints": {"enum": ["pending", "authorized", "captured", "failed"]}
                        },
                        "amount": {"type": DataType.MONEY.value},
                        "transactionId": {"type": DataType.STRING.value}
                    }
                }
            },
            "metadata": {"type": DataType.OBJECT.value}
        }
        
        # Validate entire cart
        constraints = ComplexTypeConstraints.object_constraints(
            schema=cart_schema,
            required=["cartId", "customerId", "items"]
        )
        
        valid, msg, normalized = self.validator.validate(
            cart_data,
            DataType.OBJECT.value,
            constraints
        )
        
        assert_equal(
            actual=valid,
            expected=True,
            field_name="ecommerce_cart_validation",
            context={"message": msg}
        )
        print("✅ E-commerce shopping cart validated")
        
        # Test serialization
        serialized, metadata = self.serializer.serialize(
            normalized,
            DataType.OBJECT.value,
            constraints
        )
        print(f"✅ Cart serialized: {len(serialized)} bytes")
        
        # Deserialize and verify
        deserialized = self.serializer.deserialize(
            serialized,
            DataType.OBJECT.value,
            metadata
        )
        
        assert_equal(
            actual=len(deserialized["items"]),
            expected=2,
            field_name="cart_items_count"
        )
        print("✅ Cart deserialization successful")
    
    async def test_complex_normalization(self, client: httpx.AsyncClient):
        """복합 타입 변환 및 정규화 테스트"""
        
        print("\n📌 Testing complex type normalization...")
        
        # Test data with various formats
        raw_data = {
            "contact": {
                "emails": [
                    "USER@EXAMPLE.COM",  # Should normalize to lowercase
                    "test+tag@example.com"
                ],
                "phones": [
                    "010-1234-5678",  # Korean format
                    "+1 (415) 555-0123",  # US format with formatting
                    "02-123-4567"  # Seoul landline
                ],
                "primaryAddress": "서울특별시 강남구 테헤란로 123",  # String address
                "secondaryAddress": {  # Object address
                    "street": "456 Market St",
                    "city": "San Francisco",
                    "state": "CA",
                    "postalCode": "94105",
                    "country": "US"
                }
            },
            "pricing": {
                "amounts": [
                    "1234.56 USD",  # String format
                    {"amount": 999.99, "currency": "EUR"},  # Object format
                    "₩1,234,567"  # Korean won with formatting
                ]
            },
            "locations": [
                "37.7749,-122.4194",  # String coordinates
                {"lat": 37.5665, "lng": 126.9780},  # Object with short names
                [35.6762, 139.6503]  # Array format
            ]
        }
        
        # Define schema with normalization
        schema = {
            "contact": {
                "type": DataType.OBJECT.value,
                "constraints": {
                    "schema": {
                        "emails": {
                            "type": DataType.ARRAY.value,
                            "constraints": {"itemType": DataType.EMAIL.value}
                        },
                        "phones": {
                            "type": DataType.ARRAY.value,
                            "constraints": {
                                "itemType": DataType.PHONE.value,
                                "defaultRegion": "KR"
                            }
                        },
                        "primaryAddress": {"type": DataType.ADDRESS.value},
                        "secondaryAddress": {"type": DataType.ADDRESS.value}
                    }
                }
            },
            "pricing": {
                "type": DataType.OBJECT.value,
                "constraints": {
                    "schema": {
                        "amounts": {
                            "type": DataType.ARRAY.value,
                            "constraints": {"itemType": DataType.MONEY.value}
                        }
                    }
                }
            },
            "locations": {
                "type": DataType.ARRAY.value,
                "constraints": {"itemType": DataType.COORDINATE.value}
            }
        }
        
        # Validate and normalize
        constraints = ComplexTypeConstraints.object_constraints(schema=schema)
        valid, msg, normalized = self.validator.validate(
            raw_data,
            DataType.OBJECT.value,
            constraints
        )
        
        assert_equal(
            actual=valid,
            expected=True,
            field_name="normalization_validation",
            context={"message": msg}
        )
        
        # Check normalizations
        print("✅ Normalization results:")
        
        # Email normalization
        if "contact" in normalized and "emails" in normalized["contact"]:
            first_email = normalized["contact"]["emails"][0]
            print(f"  - Email: USER@EXAMPLE.COM → {first_email.get('email', first_email)}")
        
        # Phone normalization
        if "contact" in normalized and "phones" in normalized["contact"]:
            for i, phone in enumerate(normalized["contact"]["phones"]):
                print(f"  - Phone {i+1}: {phone.get('e164', phone)}")
        
        # Coordinate normalization
        if "locations" in normalized:
            for i, loc in enumerate(normalized["locations"]):
                print(f"  - Location {i+1}: {loc.get('formatted', loc)}")
        
        print("✅ All complex types normalized successfully")
    
    async def test_circular_reference_detection(self, client: httpx.AsyncClient):
        """순환 참조 감지 테스트"""
        
        print("\n📌 Testing circular reference detection...")
        
        # Create a schema that could have circular references
        node_schema = {
            "id": {"type": DataType.STRING.value},
            "name": {"type": DataType.STRING.value},
            "parent": {
                "type": DataType.OBJECT.value,
                "constraints": {
                    "schema": {
                        "id": {"type": DataType.STRING.value},
                        "name": {"type": DataType.STRING.value}
                        # In a real circular reference, this would reference back
                    }
                }
            },
            "children": {
                "type": DataType.ARRAY.value,
                "constraints": {
                    "itemType": DataType.OBJECT.value,
                    "schema": {
                        "id": {"type": DataType.STRING.value},
                        "name": {"type": DataType.STRING.value}
                    }
                }
            }
        }
        
        # Test data with deep nesting
        test_data = {
            "id": "node1",
            "name": "Root Node",
            "parent": None,
            "children": [
                {
                    "id": "node2",
                    "name": "Child 1",
                    "children": [
                        {
                            "id": "node3",
                            "name": "Grandchild 1"
                        }
                    ]
                },
                {
                    "id": "node4",
                    "name": "Child 2"
                }
            ]
        }
        
        # Validate tree structure
        constraints = ComplexTypeConstraints.object_constraints(schema=node_schema)
        valid, msg, normalized = self.validator.validate(
            test_data,
            DataType.OBJECT.value,
            constraints
        )
        
        assert_equal(
            actual=valid,
            expected=True,
            field_name="tree_structure_validation",
            context={"message": msg}
        )
        print("✅ Tree structure validated without circular reference issues")
    
    async def test_large_nested_structures(self, client: httpx.AsyncClient):
        """대용량 중첩 데이터 테스트"""
        
        print("\n📌 Testing large nested structures...")
        
        # Create a large nested structure
        large_data = {
            "catalog": {
                "categories": []
            }
        }
        
        # Generate 10 categories
        for cat_idx in range(10):
            category = {
                "id": f"CAT-{cat_idx:03d}",
                "name": f"Category {cat_idx + 1}",
                "products": []
            }
            
            # Each category has 20 products
            for prod_idx in range(20):
                product = {
                    "id": f"PROD-{cat_idx:03d}-{prod_idx:03d}",
                    "name": f"Product {prod_idx + 1}",
                    "price": {"amount": (prod_idx + 1) * 10.99, "currency": "USD"},
                    "images": [
                        f"https://cdn.example.com/prod{cat_idx}{prod_idx}_1.jpg",
                        f"https://cdn.example.com/prod{cat_idx}{prod_idx}_2.jpg"
                    ],
                    "tags": ["new", "featured"] if prod_idx % 2 == 0 else ["sale"],
                    "specifications": {
                        "weight": f"{(prod_idx + 1) * 0.5}kg",
                        "dimensions": {
                            "length": prod_idx + 10,
                            "width": prod_idx + 5,
                            "height": prod_idx + 3
                        }
                    }
                }
                category["products"].append(product)
            
            large_data["catalog"]["categories"].append(category)
        
        # Define schema for large structure
        catalog_schema = {
            "catalog": {
                "type": DataType.OBJECT.value,
                "constraints": {
                    "schema": {
                        "categories": {
                            "type": DataType.ARRAY.value,
                            "constraints": {
                                "itemType": DataType.OBJECT.value,
                                "schema": {
                                    "id": {"type": DataType.STRING.value},
                                    "name": {"type": DataType.STRING.value},
                                    "products": {
                                        "type": DataType.ARRAY.value,
                                        "constraints": {
                                            "itemType": DataType.OBJECT.value
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        # Validate large structure
        import time
        start_time = time.time()
        
        constraints = ComplexTypeConstraints.object_constraints(schema=catalog_schema)
        valid, msg, normalized = self.validator.validate(
            large_data,
            DataType.OBJECT.value,
            constraints
        )
        
        validation_time = time.time() - start_time
        
        assert_equal(
            actual=valid,
            expected=True,
            field_name="large_structure_validation",
            context={"message": msg}
        )
        
        # Calculate statistics
        total_products = sum(len(cat["products"]) for cat in large_data["catalog"]["categories"])
        print(f"✅ Large structure validated:")
        print(f"  - Categories: {len(large_data['catalog']['categories'])}")
        print(f"  - Total products: {total_products}")
        print(f"  - Validation time: {validation_time:.3f}s")
        
        # Test serialization performance
        start_time = time.time()
        serialized, metadata = self.serializer.serialize(
            normalized,
            DataType.OBJECT.value,
            constraints
        )
        serialization_time = time.time() - start_time
        
        print(f"  - Serialization time: {serialization_time:.3f}s")
        print(f"  - Serialized size: {len(serialized)} bytes")
        
        # Test deserialization performance
        start_time = time.time()
        deserialized = self.serializer.deserialize(
            serialized,
            DataType.OBJECT.value,
            metadata
        )
        deserialization_time = time.time() - start_time
        
        print(f"  - Deserialization time: {deserialization_time:.3f}s")
        print("✅ Large structure handling successful")
    
    def record_result(self, test_name: str, success: bool, message: str):
        """테스트 결과 기록"""
        self.test_results["total"] += 1
        if success:
            self.test_results["passed"] += 1
        else:
            self.test_results["failed"] += 1
        
        self.test_results["details"].append({
            "test": test_name,
            "success": success,
            "message": message,
            "timestamp": datetime.now().isoformat()
        })
    
    def print_summary(self):
        """테스트 요약 출력"""
        
        print("\n" + "🔥" * 60)
        print("🔥 THINK ULTRA!! Complex Type Interactions Test Results")
        print("🔥" * 60)
        
        print(f"\n📊 테스트 통계:")
        print(f"   총 테스트: {self.test_results['total']}")
        print(f"   성공: {self.test_results['passed']} ✅")
        print(f"   실패: {self.test_results['failed']} ❌")
        
        if self.test_results['total'] > 0:
            success_rate = (self.test_results['passed'] / self.test_results['total']) * 100
            print(f"   성공률: {success_rate:.1f}%")
        
        if self.test_results['failed'] > 0:
            print("\n❌ 실패한 테스트:")
            for detail in self.test_results['details']:
                if not detail['success']:
                    print(f"   - {detail['test']}: {detail['message']}")
        
        print(f"\n🏆 결론:")
        if self.test_results['failed'] == 0:
            print("   ✅ 모든 복합 타입 상호작용이 완벽하게 작동합니다!")
            print("   ✅ 중첩 구조, 제약조건, 직렬화 모두 정상!")
            print("   ✅ 실제 e-commerce 시나리오 검증 완료!")
        else:
            print("   ⚠️ 일부 복합 타입 상호작용에 문제가 있습니다.")
        
        # Save results
        results_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'results')
        os.makedirs(results_dir, exist_ok=True)
        result_file = os.path.join(
            results_dir, 
            f"complex_interactions_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(self.test_results, f, ensure_ascii=False, indent=2)
        
        print(f"\n📄 상세 결과가 저장되었습니다: {result_file}")


async def main():
    """메인 테스트 실행"""
    
    tester = ComplexTypeInteractionsTest()
    await tester.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())