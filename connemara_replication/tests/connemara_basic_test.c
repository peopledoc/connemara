#include <stdio.h>
#include "connemara_replication.h"
#include <CUnit/Basic.h>

void test_is_last_message() {
	CU_ASSERT_FALSE(is_last_message("{hello", 6));
	CU_ASSERT_FALSE(is_last_message("hello}there", 11));
	CU_ASSERT_FALSE(is_last_message("hello}", 6));
	CU_ASSERT_TRUE (is_last_message("]}", 2));
}


void test_is_first_message() {
	char * payload;
	size_t payloadLength;
	char * message = "{\"xid\":1234,\"timestamp\":\"1234.123\", \"changes\": [";
	CU_ASSERT_TRUE(is_first_message(message, strlen(message), &payload, &payloadLength));
	CU_ASSERT_EQUAL(payloadLength, 33);
	CU_ASSERT_NSTRING_EQUAL(payload, "\"xid\":1234,\"timestamp\":\"1234.123\"", 33);
	message = "{\"xid\":1234,\"other\": \"blah\", \"timestamp\": \"1234.123\"";
	CU_ASSERT_FALSE(is_first_message(message, strlen(message), &payload, &payloadLength));
	message = "{\"timestamp\": \"1234.123\"";
	CU_ASSERT_FALSE(is_first_message(message, strlen(message), &payload, &payloadLength));

}

int main(int argc, char **argv) {
	/* initialize the CUnit test registry */
	if (CUE_SUCCESS != CU_initialize_registry())
		return CU_get_error();

	CU_TestInfo test_array[] = {
	  { "test of is_last_message", test_is_last_message },
	  { "test of is_first_message", test_is_first_message},
	  CU_TEST_INFO_NULL,
	};

	CU_SuiteInfo suites[] = {
	  { "suite 1", NULL, NULL, NULL, NULL, test_array },
	  CU_SUITE_INFO_NULL,
	};
	CU_ErrorCode errcode =  CU_register_suites(suites);
	if (errcode != CUE_SUCCESS) {
		CU_cleanup_registry();
		return CU_get_error();
	}
	/* Run all tests using the CUnit Basic interface */
	CU_basic_set_mode(CU_BRM_VERBOSE);
	CU_basic_run_tests();
	unsigned int failures = CU_get_number_of_failures();
	CU_cleanup_registry();
	return CU_get_error() || failures;
}

