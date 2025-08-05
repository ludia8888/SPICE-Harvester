"""
Unit tests for search configuration
검색 설정 단위 테스트
"""

import pytest
from shared.config.search_config import (
    sanitize_index_name,
    get_instances_index_name,
    get_ontologies_index_name,
    get_index_alias_name
)


class TestSanitizeIndexName:
    """sanitize_index_name 함수 테스트"""
    
    def test_lowercase_conversion(self):
        """대문자를 소문자로 변환"""
        assert sanitize_index_name("MyDatabase") == "mydatabase"
        assert sanitize_index_name("TEST_DB") == "test_db"
    
    def test_space_replacement(self):
        """공백을 underscore로 변환"""
        assert sanitize_index_name("my database") == "my_database"
        assert sanitize_index_name("test  db") == "test_db"
    
    def test_special_character_replacement(self):
        """허용되지 않는 특수문자를 underscore로 변환"""
        assert sanitize_index_name("my-database") == "my-database"  # dash는 허용
        assert sanitize_index_name("my_database") == "my_database"  # underscore는 허용
        assert sanitize_index_name("my+database") == "my+database"  # plus는 허용
        assert sanitize_index_name("my@database") == "my_database"  # @는 불허
        assert sanitize_index_name("my#database") == "my_database"  # #는 불허
        assert sanitize_index_name("my$database") == "my_database"  # $는 불허
        assert sanitize_index_name("my%database") == "my_database"  # %는 불허
        assert sanitize_index_name("my&database") == "my_database"  # &는 불허
        assert sanitize_index_name("my*database") == "my_database"  # *는 불허
        assert sanitize_index_name("my(database)") == "my_database_"  # ()는 불허
    
    def test_consecutive_underscores(self):
        """연속된 underscore 제거"""
        assert sanitize_index_name("my___database") == "my_database"
        assert sanitize_index_name("test@@@@db") == "test_db"
    
    def test_leading_dot_removal(self):
        """시작 마침표 제거"""
        assert sanitize_index_name(".hidden") == "hidden"
        assert sanitize_index_name("..hidden") == "hidden"
    
    def test_trim_underscores(self):
        """시작과 끝의 underscore 제거"""
        assert sanitize_index_name("_database_") == "database"
        assert sanitize_index_name("__test__") == "test"
    
    def test_empty_string_handling(self):
        """빈 문자열 처리"""
        assert sanitize_index_name("") == "default"
        assert sanitize_index_name("___") == "default"
        assert sanitize_index_name("...") == "default"
    
    def test_length_limit(self):
        """255바이트 제한"""
        long_name = "a" * 300
        result = sanitize_index_name(long_name)
        assert len(result.encode('utf-8')) <= 255
    
    def test_unicode_handling(self):
        """유니코드 처리"""
        assert sanitize_index_name("한글데이터베이스") == "default"  # 한글은 모두 제거됨
        assert sanitize_index_name("test한글db") == "test_db"
        assert sanitize_index_name("データベース") == "default"  # 일본어도 제거
    
    def test_mixed_cases(self):
        """복합 케이스"""
        assert sanitize_index_name("My-Test DB@2023!") == "my-test_db_2023"
        assert sanitize_index_name("___My---Database___") == "my---database"
        assert sanitize_index_name(".@#$%^&*()") == "default"


class TestGetInstancesIndexName:
    """get_instances_index_name 함수 테스트"""
    
    def test_basic_index_name(self):
        """기본 인덱스 이름 생성"""
        assert get_instances_index_name("mydb") == "mydb_instances"
        assert get_instances_index_name("test-db") == "test-db_instances"
    
    def test_with_special_characters(self):
        """특수문자가 포함된 데이터베이스 이름"""
        assert get_instances_index_name("My Database") == "my_database_instances"
        assert get_instances_index_name("test@db") == "test_db_instances"
        assert get_instances_index_name("DB#2023") == "db_2023_instances"
    
    def test_with_version(self):
        """버전이 포함된 인덱스 이름"""
        assert get_instances_index_name("mydb", "v1") == "mydb_instances_v1"
        assert get_instances_index_name("mydb", "v2") == "mydb_instances_v2"
        assert get_instances_index_name("test-db", "2023.10") == "test-db_instances_2023_10"
    
    def test_edge_cases(self):
        """엣지 케이스"""
        assert get_instances_index_name("") == "default_instances"
        assert get_instances_index_name("___") == "default_instances"
        assert get_instances_index_name(".hidden") == "hidden_instances"


class TestGetOntologiesIndexName:
    """get_ontologies_index_name 함수 테스트"""
    
    def test_basic_index_name(self):
        """기본 인덱스 이름 생성"""
        assert get_ontologies_index_name("mydb") == "mydb_ontologies"
        assert get_ontologies_index_name("test-db") == "test-db_ontologies"
    
    def test_with_special_characters(self):
        """특수문자가 포함된 데이터베이스 이름"""
        assert get_ontologies_index_name("My Database") == "my_database_ontologies"
        assert get_ontologies_index_name("test@db") == "test_db_ontologies"
        assert get_ontologies_index_name("DB#2023") == "db_2023_ontologies"
    
    def test_with_version(self):
        """버전이 포함된 인덱스 이름"""
        assert get_ontologies_index_name("mydb", "v1") == "mydb_ontologies_v1"
        assert get_ontologies_index_name("mydb", "v2") == "mydb_ontologies_v2"
        assert get_ontologies_index_name("test-db", "2023.10") == "test-db_ontologies_2023_10"


class TestGetIndexAliasName:
    """get_index_alias_name 함수 테스트"""
    
    def test_version_removal(self):
        """버전 접미사 제거"""
        assert get_index_alias_name("mydb_instances_v1") == "mydb_instances"
        assert get_index_alias_name("mydb_instances_v2") == "mydb_instances"
        assert get_index_alias_name("test_ontologies_v10") == "test_ontologies"
    
    def test_no_version(self):
        """버전이 없는 경우"""
        assert get_index_alias_name("mydb_instances") == "mydb_instances"
        assert get_index_alias_name("test_ontologies") == "test_ontologies"
    
    def test_edge_cases(self):
        """엣지 케이스"""
        assert get_index_alias_name("mydb_v1_instances_v2") == "mydb_v1_instances"
        assert get_index_alias_name("test_v") == "test_v"  # 불완전한 버전 패턴
        assert get_index_alias_name("db_va") == "db_va"  # 버전이 아닌 경우


class TestRealWorldScenarios:
    """실제 사용 시나리오 테스트"""
    
    def test_korean_database_names(self):
        """한글 데이터베이스 이름"""
        assert get_instances_index_name("고객관리") == "default_instances"
        assert get_instances_index_name("customer_고객") == "customer_instances"
    
    def test_common_patterns(self):
        """일반적인 패턴"""
        # 날짜가 포함된 이름
        assert get_instances_index_name("db-2023-10-20") == "db-2023-10-20_instances"
        
        # 환경 구분
        assert get_instances_index_name("prod-myapp") == "prod-myapp_instances"
        assert get_instances_index_name("dev_myapp") == "dev_myapp_instances"
        
        # 회사/프로젝트 이름
        assert get_instances_index_name("acme-corp") == "acme-corp_instances"
        assert get_instances_index_name("project_alpha") == "project_alpha_instances"
    
    def test_migration_scenarios(self):
        """마이그레이션 시나리오"""
        db_name = "customer-db"
        
        # Blue-Green 배포
        blue_index = get_instances_index_name(db_name, "blue")
        green_index = get_instances_index_name(db_name, "green")
        
        assert blue_index == "customer-db_instances_blue"
        assert green_index == "customer-db_instances_green"
        assert get_index_alias_name(blue_index) == "customer-db_instances"
        assert get_index_alias_name(green_index) == "customer-db_instances"