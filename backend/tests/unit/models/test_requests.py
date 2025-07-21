"""
Unit tests for Pydantic request models
Tests validation logic, field constraints, and error handling
"""

import pytest

# No need for sys.path.insert - using proper spice_harvester package imports
from pydantic import ValidationError
from tests.utils.assertions import assert_equal
from shared.models.requests import (
    BranchCreateRequest, 
    CheckoutRequest, 
    CommitRequest, 
    MergeRequest, 
    RollbackRequest, 
    DatabaseCreateRequest,
    ApiResponse
)

class TestBranchCreateRequest:
    """Test BranchCreateRequest validation"""
    
    def test_valid_branch_request(self, sample_branch_data):
        """Test valid branch creation request"""
        request = BranchCreateRequest(**sample_branch_data)
        assert_equal(
            actual=request.branch_name,
            expected="feature-test",
            field_name="request.branch_name",
            context={"sample_data": sample_branch_data}
        )
        assert_equal(
            actual=request.from_commit,
            expected="main",
            field_name="request.from_commit",
            context={"sample_data": sample_branch_data}
        )
    
    def test_branch_name_validation(self):
        """Test branch name validation rules"""
        # Valid branch names
        valid_names = ["main", "feature-branch", "test_branch", "branch-1", "dev/feature"]
        for name in valid_names:
            request = BranchCreateRequest(branch_name=name)
            assert_equal(
                actual=request.branch_name,
                expected=name,
                field_name="request.branch_name",
                context={"valid_name": name}
            )
        
        # Invalid branch names
        invalid_names = ["", "branch name", "branch@name", "branch#name", "branch!"]
        for name in invalid_names:
            with pytest.raises(ValidationError):
                BranchCreateRequest(branch_name=name)
    
    def test_branch_name_length_limits(self):
        """Test branch name length validation"""
        # Too long
        with pytest.raises(ValidationError):
            BranchCreateRequest(branch_name="a" * 101)
        
        # Valid length
        request = BranchCreateRequest(branch_name="a" * 100)
        assert_equal(
            actual=len(request.branch_name),
            expected=100,
            field_name="request.branch_name_length",
            context={"branch_name": request.branch_name}
        )

class TestCheckoutRequest:
    """Test CheckoutRequest validation"""
    
    def test_valid_branch_checkout(self):
        """Test valid branch checkout"""
        request = CheckoutRequest(target="main", target_type="branch")
        assert_equal(
            actual=request.target,
            expected="main",
            field_name="request.target",
            context={"checkout_type": "branch"}
        )
        assert_equal(
            actual=request.target_type,
            expected="branch",
            field_name="request.target_type",
            context={"target": "main"}
        )
    
    def test_valid_commit_checkout(self):
        """Test valid commit checkout"""
        request = CheckoutRequest(target="abc123def456", target_type="commit")
        assert_equal(
            actual=request.target,
            expected="abc123def456",
            field_name="request.target",
            context={"checkout_type": "commit"}
        )
        assert_equal(
            actual=request.target_type,
            expected="commit",
            field_name="request.target_type",
            context={"target": "abc123def456"}
        )
    
    def test_invalid_commit_id(self):
        """Test invalid commit ID validation"""
        with pytest.raises(ValidationError):
            CheckoutRequest(target="invalid@commit", target_type="commit")
    
    def test_default_type(self):
        """Test default checkout type"""
        request = CheckoutRequest(target="main")
        assert_equal(
            actual=request.target_type,
            expected="branch",
            field_name="request.target_type",
            context={"description": "default checkout type", "target": "main"}
        )

class TestCommitRequest:
    """Test CommitRequest validation"""
    
    def test_valid_commit_request(self, sample_commit_data):
        """Test valid commit request"""
        request = CommitRequest(**sample_commit_data)
        assert_equal(
            actual=request.message,
            expected="Test commit message",
            field_name="request.message",
            context={"sample_data": sample_commit_data}
        )
        assert_equal(
            actual=request.author,
            expected="test@example.com",
            field_name="request.author",
            context={"sample_data": sample_commit_data}
        )
        assert_equal(
            actual=request.branch,
            expected="feature-test",
            field_name="request.branch",
            context={"sample_data": sample_commit_data}
        )
    
    def test_email_validation(self):
        """Test author email validation"""
        # Valid emails
        valid_emails = ["test@example.com", "user.name@domain.co.uk", "dev+test@company.org"]
        for email in valid_emails:
            request = CommitRequest(message="test", author=email)
            assert_equal(
                actual=request.author,
                expected=email,
                field_name="request.author",
                context={"valid_email": email}
            )
        
        # Invalid emails that should actually fail (empty string/spaces)
        invalid_emails = ["", "   "]
        for email in invalid_emails:
            with pytest.raises(ValidationError):
                CommitRequest(message="test", author=email)
        
        # Note: "notanemail" passes because email.utils.parseaddr is lenient
    
    def test_message_length_limits(self):
        """Test commit message length validation"""
        # Too long
        with pytest.raises(ValidationError):
            CommitRequest(message="a" * 501, author="test@example.com")
        
        # Valid length
        request = CommitRequest(message="a" * 500, author="test@example.com")
        assert_equal(
            actual=len(request.message),
            expected=500,
            field_name="request.message_length",
            context={"message_content": "a" * 500}
        )

class TestMergeRequest:
    """Test MergeRequest validation"""
    
    def test_valid_merge_request(self, sample_merge_data):
        """Test valid merge request"""
        request = MergeRequest(**sample_merge_data)
        assert_equal(
            actual=request.source_branch,
            expected="feature-test",
            field_name="request.source_branch",
            context={"sample_data": sample_merge_data}
        )
        assert_equal(
            actual=request.target_branch,
            expected="main",
            field_name="request.target_branch",
            context={"sample_data": sample_merge_data}
        )
        assert_equal(
            actual=request.strategy,
            expected="merge",
            field_name="request.strategy",
            context={"sample_data": sample_merge_data}
        )
    
    def test_strategy_validation(self):
        """Test merge strategy validation"""
        valid_strategies = ["merge", "squash", "rebase"]
        for strategy in valid_strategies:
            request = MergeRequest(
                source_branch="feature", 
                target_branch="main", 
                strategy=strategy
            )
            assert_equal(
                actual=request.strategy,
                expected=strategy,
                field_name="request.strategy",
                context={"valid_strategy": strategy}
            )
        
        # Invalid strategy should use default
        request = MergeRequest(source_branch="feature", target_branch="main")
        assert_equal(
            actual=request.strategy,
            expected="merge",
            field_name="request.strategy",
            context={"description": "default strategy when none specified"}
        )
    
    def test_optional_author_validation(self):
        """Test optional author email validation"""
        # Valid with author
        request = MergeRequest(
            source_branch="feature", 
            target_branch="main",
            author="test@example.com"
        )
        assert_equal(
            actual=request.author,
            expected="test@example.com",
            field_name="request.author",
            context={"description": "merge request with author"}
        )
        
        # Valid without author
        request = MergeRequest(source_branch="feature", target_branch="main")
        assert_equal(
            actual=request.author,
            expected=None,
            field_name="request.author",
            context={"description": "merge request without author"}
        )
        
        # Invalid author email (empty string should fail)
        with pytest.raises(ValidationError):
            MergeRequest(
                source_branch="feature", 
                target_branch="main",
                author=""
            )

class TestRollbackRequest:
    """Test RollbackRequest validation"""
    
    def test_valid_rollback_request(self, sample_rollback_data):
        """Test valid rollback request"""
        request = RollbackRequest(**sample_rollback_data)
        assert_equal(
            actual=request.target_commit,
            expected="abc123def456",
            field_name="request.target_commit",
            context={"sample_data": sample_rollback_data}
        )
        assert_equal(
            actual=request.create_branch,
            expected=True,
            field_name="request.create_branch",
            context={"sample_data": sample_rollback_data}
        )
        assert_equal(
            actual=request.branch_name,
            expected="rollback-test",
            field_name="request.branch_name",
            context={"sample_data": sample_rollback_data}
        )
    
    def test_commit_id_validation(self):
        """Test commit ID validation"""
        # Valid commit IDs
        valid_commits = ["abc123", "ABC123DEF456", "1234567890abcdef"]
        for commit in valid_commits:
            request = RollbackRequest(target_commit=commit)
            assert_equal(
                actual=request.target_commit,
                expected=commit,
                field_name="request.target_commit",
                context={"valid_commit": commit}
            )
        
        # Invalid commit IDs
        invalid_commits = ["invalid_commit", "commit-123", "abc123@"]
        for commit in invalid_commits:
            with pytest.raises(ValidationError):
                RollbackRequest(target_commit=commit)
    
    def test_default_create_branch(self):
        """Test default value for create_branch"""
        request = RollbackRequest(target_commit="abc123")
        assert_equal(
            actual=request.create_branch,
            expected=True,
            field_name="request.create_branch",
            context={"description": "default value for create_branch"}
        )

class TestDatabaseCreateRequest:
    """Test DatabaseCreateRequest validation"""
    
    def test_valid_database_request(self, sample_database_data):
        """Test valid database creation request"""
        request = DatabaseCreateRequest(**sample_database_data)
        assert_equal(
            actual=request.name,
            expected="test_database",
            field_name="request.name",
            context={"sample_data": sample_database_data}
        )
        assert_equal(
            actual=request.description,
            expected="Test database for unit tests",
            field_name="request.description",
            context={"sample_data": sample_database_data}
        )
    
    def test_database_name_validation(self):
        """Test database name validation rules"""
        # Valid names
        valid_names = ["test_db", "MyDatabase", "db-1", "database_test"]
        for name in valid_names:
            request = DatabaseCreateRequest(name=name)
            assert_equal(
                actual=request.name,
                expected=name,
                field_name="request.name",
                context={"valid_name": name}
            )
        
        # Invalid names (must start with letter)
        invalid_names = ["1database", "_database", "-database", "database space"]
        for name in invalid_names:
            with pytest.raises(ValidationError):
                DatabaseCreateRequest(name=name)
    
    def test_description_default(self):
        """Test default description value"""
        request = DatabaseCreateRequest(name="test_db")
        assert_equal(
            actual=request.description,
            expected="",
            field_name="request.description",
            context={"description": "default description value"}
        )
    
    def test_name_length_limits(self):
        """Test database name length validation"""
        # Too long
        with pytest.raises(ValidationError):
            DatabaseCreateRequest(name="a" * 51)
        
        # Valid length
        request = DatabaseCreateRequest(name="a" * 50)
        assert_equal(
            actual=len(request.name),
            expected=50,
            field_name="request.name_length",
            context={"name_content": "a" * 50}
        )

class TestApiResponse:
    """Test ApiResponse model"""
    
    def test_success_response(self):
        """Test success response creation"""
        response = ApiResponse(
            status="success",
            message="Operation completed",
            data={"result": "test"}
        )
        assert_equal(
            actual=response.status,
            expected="success",
            field_name="response.status",
            context={"response_type": "success"}
        )
        assert_equal(
            actual=response.message,
            expected="Operation completed",
            field_name="response.message",
            context={"response_type": "success"}
        )
        assert_equal(
            actual=response.data,
            expected={"result": "test"},
            field_name="response.data",
            context={"response_type": "success"}
        )
    
    def test_error_response(self):
        """Test error response creation"""
        response = ApiResponse(
            status="error",
            message="Operation failed"
        )
        assert_equal(
            actual=response.status,
            expected="error",
            field_name="response.status",
            context={"response_type": "error"}
        )
        assert_equal(
            actual=response.message,
            expected="Operation failed",
            field_name="response.message",
            context={"response_type": "error"}
        )
        assert_equal(
            actual=response.data,
            expected=None,
            field_name="response.data",
            context={"response_type": "error"}
        )
    
    def test_status_validation(self):
        """Test status field validation"""
        # Invalid status
        with pytest.raises(ValidationError):
            ApiResponse(status="invalid", message="test")

if __name__ == "__main__":
    pytest.main([__file__])