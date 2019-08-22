include_directories(${THRIFT_INCLUDE_DIR}
        src)

add_library(jiffy STATIC
        src/jiffy/client/jiffy_client.cc
        src/jiffy/client/jiffy_client.h
        src/jiffy/directory/client/directory_client.cpp
        src/jiffy/directory/client/directory_client.h
        src/jiffy/directory/fs/directory_service.cpp
        src/jiffy/directory/fs/directory_service.h
        src/jiffy/directory/fs/directory_service.tcc
        src/jiffy/directory/fs/directory_service_constants.cpp
        src/jiffy/directory/fs/directory_service_constants.h
        src/jiffy/directory/fs/directory_service_types.cpp
        src/jiffy/directory/fs/directory_service_types.h
        src/jiffy/directory/fs/directory_service_types.tcc
        src/jiffy/directory/fs/directory_type_conversions.h
        src/jiffy/directory/client/lease_client.cpp
        src/jiffy/directory/client/lease_client.h
        src/jiffy/directory/client/lease_renewal_worker.cc
        src/jiffy/directory/client/lease_renewal_worker.h
        src/jiffy/directory/lease/lease_service.cpp
        src/jiffy/directory/lease/lease_service.h
        src/jiffy/directory/lease/lease_service.tcc
        src/jiffy/directory/lease/lease_service_constants.cpp
        src/jiffy/directory/lease/lease_service_constants.h
        src/jiffy/directory/lease/lease_service_types.cpp
        src/jiffy/directory/lease/lease_service_types.h
        src/jiffy/directory/lease/lease_service_types.tcc
        src/jiffy/directory/directory_ops.h
        src/jiffy/directory/directory_ops.cpp
        src/jiffy/storage/command.h
        src/jiffy/storage/command.cpp
        src/jiffy/storage/default/default_partition.h
        src/jiffy/storage/default/default_partition.cpp
        src/jiffy/storage/hashtable/hash_slot.h
        src/jiffy/storage/hashtable/hash_slot.cpp
        src/jiffy/storage/hashtable/hash_table_ops.h
        src/jiffy/storage/hashtable/hash_table_ops.cpp
        src/jiffy/storage/file/file_ops.h
        src/jiffy/storage/file/file_ops.cpp
        src/jiffy/storage/file/file_block.h
        src/jiffy/storage/file/file_block.cpp
        src/jiffy/storage/fifoqueue/fifo_queue_ops.h
        src/jiffy/storage/fifoqueue/fifo_queue_ops.cpp
        src/jiffy/storage/fifoqueue/string_array.h
        src/jiffy/storage/fifoqueue/string_array.cpp
        src/jiffy/storage/client/replica_chain_client.cpp
        src/jiffy/storage/client/replica_chain_client.h
        src/jiffy/storage/service/block_response_client.cpp
        src/jiffy/storage/service/block_response_client.h
        src/jiffy/storage/client/block_client.cpp
        src/jiffy/storage/client/block_client.h
        src/jiffy/storage/client/block_listener.cpp
        src/jiffy/storage/client/block_listener.h
        src/jiffy/storage/client/data_structure_client.cpp
        src/jiffy/storage/client/data_structure_client.h
        src/jiffy/storage/client/hash_table_client.cpp
        src/jiffy/storage/client/hash_table_client.h
        src/jiffy/storage/client/file_client.cpp
        src/jiffy/storage/client/file_client.h
        src/jiffy/storage/client/file_reader.cpp
        src/jiffy/storage/client/file_reader.h
        src/jiffy/storage/client/file_writer.cpp
        src/jiffy/storage/client/file_writer.h
        src/jiffy/storage/client/fifo_queue_client.cpp
        src/jiffy/storage/client/fifo_queue_client.h
        src/jiffy/storage/client/fifo_queue_client.cpp
        src/jiffy/storage/client/fifo_queue_client.h
        src/jiffy/storage/client/data_structure_listener.cpp
        src/jiffy/storage/client/data_structure_listener.h
        src/jiffy/storage/service/block_request_service.cpp
        src/jiffy/storage/service/block_request_service.h
        src/jiffy/storage/service/block_request_service.tcc
        src/jiffy/storage/service/block_response_service.cpp
        src/jiffy/storage/service/block_response_service.h
        src/jiffy/storage/service/block_response_service.tcc
        src/jiffy/storage/service/block_service_constants.cpp
        src/jiffy/storage/service/block_service_constants.h
        src/jiffy/storage/service/block_service_types.cpp
        src/jiffy/storage/service/block_service_types.h
        src/jiffy/storage/service/block_service_types.tcc
        src/jiffy/storage/manager/detail/block_id_parser.cpp
        src/jiffy/storage/manager/detail/block_id_parser.h
        src/jiffy/storage/notification/blocking_queue.h
        src/jiffy/storage/notification/notification_handler.cpp
        src/jiffy/storage/notification/notification_handler.h
        src/jiffy/storage/notification/notification_worker.cpp
        src/jiffy/storage/notification/notification_worker.h
        src/jiffy/utils/byte_utils.h
        src/jiffy/utils/client_cache.h
        src/jiffy/utils/cmd_parse.h
        src/jiffy/utils/directory_utils.h
        src/jiffy/utils/event.h
        src/jiffy/utils/logger.h
        src/jiffy/utils/logger.cpp
        src/jiffy/utils/rand_utils.h
        src/jiffy/utils/signal_handling.h
        src/jiffy/utils/time_utils.h
        src/jiffy/utils/retry_utils.h
        src/jiffy/utils/string_utils.h
        src/jiffy/utils/property_map.cpp
        src/jiffy/utils/property_map.h
        src/jiffy/utils/thread_utils.h)

if (NOT USE_SYSTEM_LIBEVENT)
  add_dependencies(jiffy libevent)
endif()

if (NOT USE_SYSTEM_THRIFT)
  add_dependencies(jiffy thrift)
endif()

if (NOT USE_SYSTEM_LIBCUCKOO)
  add_dependencies(jiffy libcuckoo)
endif ()

target_link_libraries(jiffy ${THRIFT_LIBRARIES}
        ${THRIFTNB_LIBRARIES}
        ${LIBEVENT_LIBRARIES}
        ${OPENSSL_LIBRARIES}
        ${ZLIB_LIBRARY}
        ${CMAKE_DL_LIBS}
        ${CMAKE_THREAD_LIBS_INIT})
set_target_properties(jiffy PROPERTIES LINKER_LANGUAGE CXX)

install(TARGETS jiffy
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(TARGETS jiffy
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(DIRECTORY src/jiffy
        DESTINATION include
        FILES_MATCHING PATTERN "*.h")