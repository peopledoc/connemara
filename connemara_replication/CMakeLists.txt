cmake_minimum_required(VERSION 3.0)

project(connemara_replication)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
find_package (PkgConfig REQUIRED)
pkg_check_modules(PCRE REQUIRED libpcre)

find_package (PostgreSQL REQUIRED)

add_definitions("-D_GNU_SOURCE -Wall -Werror")
add_executable(connemara_replication src/connemara_replication.c src/log.c src/main.c)
target_include_directories(connemara_replication PUBLIC ${PostgreSQL_INCLUDE_DIRS})
target_link_libraries(connemara_replication PUBLIC ${PostgreSQL_LIBRARIES} ${PCRE_LIBRARIES})

install(TARGETS ${CMAKE_PROJECT_NAME}
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib)

enable_testing()
find_package(CUnit REQUIRED)
add_executable(connemara_basic_test src/connemara_replication.c src/log.c tests/connemara_basic_test.c)
target_include_directories(connemara_basic_test PUBLIC ${PostgreSQL_INCLUDE_DIRS} "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_link_libraries(connemara_basic_test PUBLIC ${PostgreSQL_LIBRARIES} ${PCRE_LIBRARIES} ${CUNIT_LIBRARIES})
add_test(NAME connemera_basic_test COMMAND connemara_basic_test)


SET(MAJOR_VERSION 1)
SET(MINOR_VERSION 2)
SET(PATCH_VERSION 0)



add_definitions("-DCONNEMARA_VERSION=\"${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}\"")

IF(EXISTS "${CMAKE_ROOT}/Modules/CPack.cmake")
INCLUDE(InstallRequiredSystemLibraries)

SET(CPACK_SET_DESTDIR "on")
SET(CPACK_PACKAGING_INSTALL_PREFIX "/tmp")
SET(CPACK_GENERATOR "DEB")
set(CPACK_INSTALL_PREFIX "/usr/")


SET(CPACK_PACKAGE_DESCRIPTION "Connemara replication")
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "A logical replication client")
SET(CPACK_PACKAGE_VENDOR "PeopleDoc")
SET(CPACK_PACKAGE_CONTACT "dba@people-doc.com")
SET(CPACK_PACKAGE_VERSION_MAJOR "${MAJOR_VERSION}")
SET(CPACK_PACKAGE_VERSION_MINOR "${MINOR_VERSION}")
SET(CPACK_PACKAGE_VERSION_PATCH "${PATCH_VERSION}")
SET(CPACK_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}_${MAJOR_VERSION}.${MINOR_VERSION}.${CPACK_PACKAGE_VERSION_PATCH}")
SET(CPACK_SOURCE_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}_${MAJOR_VERSION}.${MINOR_VERSION}.${CPACK_PACKAGE_VERSION_PATCH}")

SET(CPACK_DEBIAN_PACKAGE_DEPENDS "libpq5")

SET(CPACK_DEBIAN_PACKAGE_PRIORITY "optional")
SET(CPACK_DEBIAN_PACKAGE_SECTION "database")
SET(CPACK_DEBIAN_ARCHITECTURE ${CMAKE_SYSTEM_PROCESSOR})

SET(CPACK_COMPONENTS_ALL Libraries ApplicationData)
INCLUDE(CPack)

ENDIF(EXISTS "${CMAKE_ROOT}/Modules/CPack.cmake")
