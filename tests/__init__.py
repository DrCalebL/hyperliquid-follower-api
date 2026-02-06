"""
Nike Rocket Test Suite
======================

Run all tests:
    pytest tests/ -v

Run specific test file:
    pytest tests/test_billing_integration.py -v

Run specific test:
    pytest tests/test_billing_integration.py::TestBillingCycles::test_profitable_cycle_standard_tier -v

Environment Variables Required:
    TEST_DATABASE_URL - PostgreSQL connection string for test database
    
Example:
    export TEST_DATABASE_URL="postgresql://user:pass@localhost:5432/nikerocket_test"
    pytest tests/ -v
"""
