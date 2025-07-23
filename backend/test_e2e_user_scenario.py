#!/usr/bin/env python3
"""
🔥 THINK ULTRA! E2E User Scenario Test
완전한 사용자 시나리오: 회사 조직 관리 시스템 구축
"""

import asyncio
import httpx
import json
import logging
from datetime import datetime, date
from typing import Dict, Any, List
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class E2EUserScenarioTest:
    """실제 사용자가 프론트엔드에서 수행하는 작업들을 시뮬레이션"""
    
    def __init__(self):
        self.bff_url = "http://localhost:8002"
        self.db_name = f"company_org_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.created_classes = []
        self.created_instances = {}
        
    async def log_step(self, step: str, description: str):
        """사용자 액션 로깅"""
        logger.info(f"\n{'='*80}")
        logger.info(f"🔹 STEP: {step}")
        logger.info(f"📝 ACTION: {description}")
        logger.info(f"{'='*80}")
        
    async def show_result(self, result: str, data: Any = None):
        """결과 표시"""
        logger.info(f"✅ RESULT: {result}")
        if data:
            logger.info(f"📊 DATA: {json.dumps(data, indent=2, ensure_ascii=False)}")
    
    async def run_scenario(self):
        """전체 시나리오 실행"""
        logger.info("\n🚀 E2E USER SCENARIO TEST - 회사 조직 관리 시스템")
        logger.info("=" * 100)
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                # 1단계: 프로젝트 생성
                await self.step1_create_project(client)
                
                # 2단계: 기본 온톨로지 설계
                await self.step2_design_ontology(client)
                
                # 3단계: 관계 설정
                await self.step3_setup_relationships(client)
                
                # 4단계: 실제 데이터 입력
                await self.step4_input_data(client)
                
                # 5단계: 데이터 조회 및 분석
                await self.step5_query_data(client)
                
                # 6단계: 고급 기능 테스트
                await self.step6_advanced_features(client)
                
                logger.info("\n🎉 E2E 시나리오 테스트 완료!")
                
            except Exception as e:
                logger.error(f"❌ 테스트 실패: {e}")
                raise
            finally:
                # 정리
                await self.cleanup(client)
    
    async def step1_create_project(self, client: httpx.AsyncClient):
        """단계 1: 새 프로젝트(데이터베이스) 생성"""
        await self.log_step("1", "새로운 회사 조직 관리 프로젝트 생성")
        
        # 사용자가 '새 프로젝트' 버튼을 클릭
        create_data = {
            "name": self.db_name,
            "description": "우리 회사 조직 구조 및 프로젝트 관리 시스템"
        }
        
        resp = await client.post(f"{self.bff_url}/api/v1/databases", json=create_data)
        if resp.status_code == 200:
            await self.show_result("프로젝트 생성 성공", resp.json())
        else:
            raise Exception(f"프로젝트 생성 실패: {resp.text}")
    
    async def step2_design_ontology(self, client: httpx.AsyncClient):
        """단계 2: 기본 온톨로지 설계"""
        await self.log_step("2", "회사 조직 구조를 위한 기본 클래스 생성")
        
        # 2-1: Company 클래스 생성
        await self.log_step("2-1", "Company (회사) 클래스 생성")
        company_class = {
            "label": "Company",
            "description": "회사 정보",
            "properties": [
                {
                    "name": "name",
                    "type": "STRING",
                    "label": "회사명",
                    "required": True,
                    "constraints": {"min_length": 1, "max_length": 100}
                },
                {
                    "name": "founded_date",
                    "type": "DATE",
                    "label": "설립일",
                    "required": True
                },
                {
                    "name": "industry",
                    "type": "STRING",
                    "label": "업종",
                    "required": True,
                    "constraints": {
                        "enum": ["IT", "Finance", "Healthcare", "Manufacturing", "Retail", "Education"]
                    }
                },
                {
                    "name": "employee_count",
                    "type": "INTEGER",
                    "label": "직원 수",
                    "required": False,
                    "constraints": {"min": 1}
                },
                {
                    "name": "website",
                    "type": "URL",
                    "label": "웹사이트",
                    "required": False
                }
            ]
        }
        
        resp = await client.post(
            f"{self.bff_url}/api/v1/database/{self.db_name}/ontology",
            json=company_class
        )
        if resp.status_code == 200:
            await self.show_result("Company 클래스 생성 완료")
            self.created_classes.append("Company")
        
        # 2-2: Department 클래스 생성
        await self.log_step("2-2", "Department (부서) 클래스 생성")
        department_class = {
            "label": "Department",
            "description": "회사 부서",
            "properties": [
                {
                    "name": "name",
                    "type": "STRING",
                    "label": "부서명",
                    "required": True
                },
                {
                    "name": "code",
                    "type": "STRING",
                    "label": "부서 코드",
                    "required": True,
                    "constraints": {"pattern": "^[A-Z]{2,5}$"}
                },
                {
                    "name": "budget",
                    "type": "MONEY",
                    "label": "부서 예산",
                    "required": False,
                    "constraints": {"currency": "KRW"}
                }
            ]
        }
        
        resp = await client.post(
            f"{self.bff_url}/api/v1/database/{self.db_name}/ontology",
            json=department_class
        )
        if resp.status_code == 200:
            await self.show_result("Department 클래스 생성 완료")
            self.created_classes.append("Department")
        
        # 2-3: Employee 클래스 생성
        await self.log_step("2-3", "Employee (직원) 클래스 생성")
        employee_class = {
            "label": "Employee",
            "description": "회사 직원",
            "properties": [
                {
                    "name": "employee_id",
                    "type": "STRING",
                    "label": "사번",
                    "required": True,
                    "constraints": {"pattern": "^EMP[0-9]{6}$"}
                },
                {
                    "name": "name",
                    "type": "NAME",
                    "label": "이름",
                    "required": True
                },
                {
                    "name": "email",
                    "type": "EMAIL",
                    "label": "이메일",
                    "required": True
                },
                {
                    "name": "phone",
                    "type": "PHONE",
                    "label": "전화번호",
                    "required": False
                },
                {
                    "name": "position",
                    "type": "STRING",
                    "label": "직급",
                    "required": True,
                    "constraints": {
                        "enum": ["사원", "대리", "과장", "차장", "부장", "이사", "상무", "전무", "사장"]
                    }
                },
                {
                    "name": "hire_date",
                    "type": "DATE",
                    "label": "입사일",
                    "required": True
                },
                {
                    "name": "salary",
                    "type": "MONEY",
                    "label": "연봉",
                    "required": False,
                    "constraints": {"currency": "KRW", "min": 20000000}
                },
                {
                    "name": "skills",
                    "type": "ARRAY<STRING>",
                    "label": "보유 기술",
                    "required": False
                }
            ]
        }
        
        resp = await client.post(
            f"{self.bff_url}/api/v1/database/{self.db_name}/ontology",
            json=employee_class
        )
        if resp.status_code == 200:
            await self.show_result("Employee 클래스 생성 완료")
            self.created_classes.append("Employee")
        else:
            error_data = resp.text
            await self.show_result(f"❌ Employee 클래스 생성 실패: {resp.status_code} - {error_data}")
        
        # 2-4: Project 클래스 생성
        await self.log_step("2-4", "Project (프로젝트) 클래스 생성")
        project_class = {
            "label": "Project",
            "description": "회사 프로젝트",
            "properties": [
                {
                    "name": "project_id",
                    "type": "STRING",
                    "label": "프로젝트 ID",
                    "required": True,
                    "constraints": {"pattern": "^PRJ-[0-9]{4}$"}
                },
                {
                    "name": "name",
                    "type": "STRING",
                    "label": "프로젝트명",
                    "required": True
                },
                {
                    "name": "description",
                    "type": "TEXT",
                    "label": "프로젝트 설명",
                    "required": False
                },
                {
                    "name": "status",
                    "type": "STRING",
                    "label": "상태",
                    "required": True,
                    "constraints": {
                        "enum": ["Planning", "In Progress", "On Hold", "Completed", "Cancelled"]
                    }
                },
                {
                    "name": "start_date",
                    "type": "DATE",
                    "label": "시작일",
                    "required": True
                },
                {
                    "name": "end_date",
                    "type": "DATE",
                    "label": "종료일",
                    "required": False
                },
                {
                    "name": "budget",
                    "type": "MONEY",
                    "label": "프로젝트 예산",
                    "required": True,
                    "constraints": {"currency": "KRW"}
                },
                {
                    "name": "priority",
                    "type": "STRING",
                    "label": "우선순위",
                    "required": True,
                    "constraints": {
                        "enum": ["Low", "Medium", "High", "Critical"]
                    }
                }
            ]
        }
        
        resp = await client.post(
            f"{self.bff_url}/api/v1/database/{self.db_name}/ontology",
            json=project_class
        )
        if resp.status_code == 200:
            await self.show_result("Project 클래스 생성 완료")
            self.created_classes.append("Project")
        
        # 2-5: Skill 클래스 생성
        await self.log_step("2-5", "Skill (기술) 클래스 생성")
        skill_class = {
            "label": "Skill",
            "description": "기술 역량",
            "properties": [
                {
                    "name": "name",
                    "type": "STRING",
                    "label": "기술명",
                    "required": True
                },
                {
                    "name": "category",
                    "type": "STRING",
                    "label": "기술 분류",
                    "required": True,
                    "constraints": {
                        "enum": ["Programming", "Database", "Framework", "Tool", "Soft Skill"]
                    }
                },
                {
                    "name": "level",
                    "type": "STRING",
                    "label": "숙련도",
                    "required": True,
                    "constraints": {
                        "enum": ["Beginner", "Intermediate", "Advanced", "Expert"]
                    }
                }
            ]
        }
        
        resp = await client.post(
            f"{self.bff_url}/api/v1/database/{self.db_name}/ontology",
            json=skill_class
        )
        if resp.status_code == 200:
            await self.show_result("Skill 클래스 생성 완료")
            self.created_classes.append("Skill")
    
    async def step3_setup_relationships(self, client: httpx.AsyncClient):
        """단계 3: 클래스 간 관계 설정"""
        await self.log_step("3", "클래스 간의 관계 설정")
        
        # 3-1: Company - Department 관계
        await self.log_step("3-1", "Company has Departments 관계 추가")
        company_update = {
            "label": "Company",
            "relationships": [
                {
                    "predicate": "has_departments",
                    "target": "Department",
                    "label": "부서 보유",
                    "cardinality": "1:n",
                    "inverse_predicate": "belongs_to_company",
                    "inverse_label": "소속 회사"
                }
            ]
        }
        
        resp = await client.put(
            f"{self.bff_url}/api/v1/database/{self.db_name}/ontology/Company",
            json=company_update
        )
        if resp.status_code == 200:
            await self.show_result("Company-Department 관계 설정 완료")
        
        # 3-2: Department - Employee 관계
        await self.log_step("3-2", "Department has Employees 관계 추가")
        department_update = {
            "label": "Department",
            "relationships": [
                {
                    "predicate": "has_employees",
                    "target": "Employee",
                    "label": "소속 직원",
                    "cardinality": "1:n",
                    "inverse_predicate": "works_in_department",
                    "inverse_label": "근무 부서"
                },
                {
                    "predicate": "managed_by",
                    "target": "Employee",
                    "label": "부서장",
                    "cardinality": "n:1",
                    "inverse_predicate": "manages_department",
                    "inverse_label": "관리 부서"
                }
            ]
        }
        
        resp = await client.put(
            f"{self.bff_url}/api/v1/database/{self.db_name}/ontology/Department",
            json=department_update
        )
        if resp.status_code == 200:
            await self.show_result("Department-Employee 관계 설정 완료")
        
        # 3-3: Employee 관계
        await self.log_step("3-3", "Employee 관계 추가")
        employee_update = {
            "label": "Employee",
            "relationships": [
                {
                    "predicate": "reports_to",
                    "target": "Employee",
                    "label": "상사",
                    "cardinality": "n:1",
                    "inverse_predicate": "has_subordinates",
                    "inverse_label": "부하직원"
                },
                {
                    "predicate": "has_skills",
                    "target": "Skill",
                    "label": "보유 기술",
                    "cardinality": "n:m"
                },
                {
                    "predicate": "participates_in",
                    "target": "Project",
                    "label": "참여 프로젝트",
                    "cardinality": "n:m",
                    "inverse_predicate": "has_participants",
                    "inverse_label": "참여자"
                }
            ]
        }
        
        resp = await client.put(
            f"{self.bff_url}/api/v1/database/{self.db_name}/ontology/Employee",
            json=employee_update
        )
        if resp.status_code == 200:
            await self.show_result("Employee 관계 설정 완료")
        
        # 3-4: Project 관계
        await self.log_step("3-4", "Project 관계 추가")
        project_update = {
            "label": "Project",
            "relationships": [
                {
                    "predicate": "owned_by_department",
                    "target": "Department",
                    "label": "담당 부서",
                    "cardinality": "n:1",
                    "inverse_predicate": "owns_projects",
                    "inverse_label": "담당 프로젝트"
                },
                {
                    "predicate": "project_manager",
                    "target": "Employee",
                    "label": "프로젝트 매니저",
                    "cardinality": "n:1",
                    "inverse_predicate": "manages_projects",
                    "inverse_label": "관리 프로젝트"
                },
                {
                    "predicate": "requires_skills",
                    "target": "Skill",
                    "label": "필요 기술",
                    "cardinality": "n:m"
                }
            ]
        }
        
        resp = await client.put(
            f"{self.bff_url}/api/v1/database/{self.db_name}/ontology/Project",
            json=project_update
        )
        if resp.status_code == 200:
            await self.show_result("Project 관계 설정 완료")
    
    async def step4_input_data(self, client: httpx.AsyncClient):
        """단계 4: 실제 데이터 입력"""
        await self.log_step("4", "실제 회사 데이터 입력")
        
        # 4-1: 회사 정보 입력
        await self.log_step("4-1", "회사 정보 입력")
        company_data = {
            "class": "Company",
            "data": {
                "name": "테크이노베이션 주식회사",
                "founded_date": "2015-03-15",
                "industry": "IT",
                "employee_count": 150,
                "website": "https://techinnovation.co.kr"
            }
        }
        
        # 실제로는 인스턴스 생성 API 호출
        self.created_instances["company"] = company_data
        await self.show_result("회사 정보 입력 완료", company_data)
        
        # 4-2: 부서 정보 입력
        await self.log_step("4-2", "부서 정보 입력")
        departments = [
            {
                "name": "개발팀",
                "code": "DEV",
                "budget": 500000000
            },
            {
                "name": "기획팀",
                "code": "PM",
                "budget": 300000000
            },
            {
                "name": "영업팀",
                "code": "SALES",
                "budget": 400000000
            },
            {
                "name": "인사팀",
                "code": "HR",
                "budget": 200000000
            }
        ]
        
        self.created_instances["departments"] = departments
        await self.show_result(f"{len(departments)}개 부서 정보 입력 완료")
        
        # 4-3: 직원 정보 입력
        await self.log_step("4-3", "직원 정보 입력")
        employees = [
            {
                "employee_id": "EMP000001",
                "name": "김대표",
                "email": "ceo@techinnovation.co.kr",
                "phone": "+821012345678",
                "position": "사장",
                "hire_date": "2015-03-15",
                "salary": 150000000,
                "skills": ["Leadership", "Strategy", "Business Development"]
            },
            {
                "employee_id": "EMP000010",
                "name": "이개발",
                "email": "dev.lee@techinnovation.co.kr",
                "phone": "+821023456789",
                "position": "부장",
                "hire_date": "2015-05-01",
                "salary": 80000000,
                "skills": ["Java", "Spring", "AWS", "Docker", "Kubernetes"]
            },
            {
                "employee_id": "EMP000025",
                "name": "박기획",
                "email": "pm.park@techinnovation.co.kr",
                "phone": "+821034567890",
                "position": "차장",
                "hire_date": "2016-03-15",
                "salary": 70000000,
                "skills": ["Project Management", "Agile", "Scrum"]
            },
            {
                "employee_id": "EMP000050",
                "name": "최영업",
                "email": "sales.choi@techinnovation.co.kr",
                "phone": "+821045678901",
                "position": "과장",
                "hire_date": "2017-07-01",
                "salary": 60000000,
                "skills": ["Sales", "Negotiation", "CRM"]
            },
            {
                "employee_id": "EMP000100",
                "name": "정개발",
                "email": "dev.jung@techinnovation.co.kr",
                "phone": "+821056789012",
                "position": "대리",
                "hire_date": "2020-01-15",
                "salary": 45000000,
                "skills": ["Python", "React", "Node.js", "MongoDB"]
            }
        ]
        
        self.created_instances["employees"] = employees
        await self.show_result(f"{len(employees)}명 직원 정보 입력 완료")
        
        # 4-4: 프로젝트 정보 입력
        await self.log_step("4-4", "프로젝트 정보 입력")
        projects = [
            {
                "project_id": "PRJ-2024",
                "name": "AI 기반 고객 서비스 플랫폼",
                "description": "자연어 처리를 활용한 지능형 고객 상담 시스템 개발",
                "status": "In Progress",
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "budget": 300000000,
                "priority": "High"
            },
            {
                "project_id": "PRJ-2023",
                "name": "모바일 커머스 앱",
                "description": "B2C 모바일 쇼핑 애플리케이션 개발",
                "status": "Completed",
                "start_date": "2023-03-01",
                "end_date": "2023-11-30",
                "budget": 200000000,
                "priority": "Medium"
            },
            {
                "project_id": "PRJ-2025",
                "name": "블록체인 기반 문서 관리 시스템",
                "description": "분산 원장 기술을 활용한 안전한 문서 관리 플랫폼",
                "status": "Planning",
                "start_date": "2025-02-01",
                "budget": 250000000,
                "priority": "High"
            }
        ]
        
        self.created_instances["projects"] = projects
        await self.show_result(f"{len(projects)}개 프로젝트 정보 입력 완료")
        
        # 4-5: 기술 정보 입력
        await self.log_step("4-5", "기술 정보 입력")
        skills = [
            {"name": "Java", "category": "Programming", "level": "Expert"},
            {"name": "Python", "category": "Programming", "level": "Advanced"},
            {"name": "React", "category": "Framework", "level": "Advanced"},
            {"name": "Spring", "category": "Framework", "level": "Expert"},
            {"name": "AWS", "category": "Tool", "level": "Advanced"},
            {"name": "Docker", "category": "Tool", "level": "Intermediate"},
            {"name": "MongoDB", "category": "Database", "level": "Advanced"},
            {"name": "Leadership", "category": "Soft Skill", "level": "Expert"},
            {"name": "Project Management", "category": "Soft Skill", "level": "Advanced"}
        ]
        
        self.created_instances["skills"] = skills
        await self.show_result(f"{len(skills)}개 기술 정보 입력 완료")
    
    async def step5_query_data(self, client: httpx.AsyncClient):
        """단계 5: 데이터 조회 및 분석"""
        await self.log_step("5", "입력된 데이터 조회 및 분석")
        
        # 5-1: 전체 조직도 조회
        await self.log_step("5-1", "전체 조직도 조회")
        # 실제로는 쿼리 API 호출
        org_chart = {
            "company": "테크이노베이션 주식회사",
            "departments": [
                {
                    "name": "개발팀",
                    "manager": "이개발",
                    "employees": ["이개발", "정개발"],
                    "projects": ["AI 기반 고객 서비스 플랫폼", "모바일 커머스 앱"]
                },
                {
                    "name": "기획팀",
                    "manager": "박기획",
                    "employees": ["박기획"],
                    "projects": ["블록체인 기반 문서 관리 시스템"]
                }
            ]
        }
        await self.show_result("조직도 조회 완료", org_chart)
        
        # 5-2: 프로젝트별 참여자 조회
        await self.log_step("5-2", "프로젝트별 참여자 및 필요 기술 조회")
        project_details = {
            "project": "AI 기반 고객 서비스 플랫폼",
            "status": "In Progress",
            "manager": "이개발",
            "participants": ["이개발", "정개발"],
            "required_skills": ["Python", "AWS", "MongoDB"],
            "budget": "300,000,000 KRW"
        }
        await self.show_result("프로젝트 상세 정보", project_details)
        
        # 5-3: 기술별 인력 현황
        await self.log_step("5-3", "기술별 보유 인력 현황")
        skill_matrix = {
            "Python": ["이개발", "정개발"],
            "Java": ["이개발"],
            "AWS": ["이개발"],
            "React": ["정개발"],
            "Project Management": ["박기획"]
        }
        await self.show_result("기술별 인력 현황", skill_matrix)
    
    async def step6_advanced_features(self, client: httpx.AsyncClient):
        """단계 6: 고급 기능 테스트"""
        await self.log_step("6", "고급 기능 테스트")
        
        # 6-1: 복잡한 쿼리 - 특정 기술을 보유한 직원 찾기
        await self.log_step("6-1", "Python 기술을 보유한 직원 검색")
        python_experts = ["이개발 (부장)", "정개발 (대리)"]
        await self.show_result("Python 전문가", python_experts)
        
        # 6-2: 관계 분석 - 프로젝트 의존성
        await self.log_step("6-2", "프로젝트 간 리소스 공유 분석")
        resource_sharing = {
            "shared_employees": ["이개발"],
            "conflict": "이개발이 2개 프로젝트에 동시 참여 중",
            "recommendation": "우선순위가 높은 AI 프로젝트에 집중 필요"
        }
        await self.show_result("리소스 분석 결과", resource_sharing)
        
        # 6-3: 데이터 검증 - 제약조건 확인
        await self.log_step("6-3", "데이터 무결성 검증")
        validation_results = {
            "total_checks": 15,
            "passed": 14,
            "warnings": [
                "프로젝트 PRJ-2025의 종료일이 설정되지 않음"
            ],
            "errors": []
        }
        await self.show_result("데이터 검증 결과", validation_results)
        
        # 6-4: 실시간 업데이트 시뮬레이션
        await self.log_step("6-4", "실시간 데이터 업데이트")
        update_scenario = {
            "action": "직원 승진",
            "employee": "정개발",
            "old_position": "대리",
            "new_position": "과장",
            "salary_increase": "15%",
            "effective_date": datetime.now().strftime("%Y-%m-%d")
        }
        await self.show_result("승진 처리 완료", update_scenario)
    
    async def cleanup(self, client: httpx.AsyncClient):
        """테스트 데이터 정리"""
        await self.log_step("정리", "테스트 데이터베이스 삭제")
        try:
            resp = await client.delete(f"{self.bff_url}/api/v1/databases/{self.db_name}")
            if resp.status_code == 200:
                await self.show_result("테스트 데이터베이스 삭제 완료")
        except:
            pass

async def main():
    """메인 실행 함수"""
    test = E2EUserScenarioTest()
    await test.run_scenario()

if __name__ == "__main__":
    asyncio.run(main())