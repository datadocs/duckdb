//
// Created by gab on 07/01/20
//

#include "simple_icu_init.h"

// STD
#include <fstream>   // std::ifstream
#include <iostream>  // std::cout, std::endl

// ICU
#include <unicode/udata.h>

int simple_icu_init( const char* icu_data_path ) {
  ///////////////////////
  // Initialize ICU Data
  ///////////////////////

  UErrorCode status = U_ZERO_ERROR;
  bool icu_init_success = false;

  // The loaded ICU data tables in static memory.
  static char* icu_data_memory = nullptr;

  if ( !icu_data_memory ) {
    // Open the ICU data file preloaded in MemFS as an ifstream
    std::ifstream icu_data_stream( std::string( icu_data_path ) + "icudt64l.dat",
                                   std::ios::in | std::ios::ate | std::ios::binary );

    // In case of issue with the path, look into the current folder
    if ( !icu_data_stream ) {
      icu_data_stream = std::ifstream ("icudt64l.dat",
                                   std::ios::in | std::ios::ate | std::ios::binary);
    }
    if ( icu_data_stream ) {
      // Get the file size (the stream was positioned at the end of the file with ios::ate)
      auto sz = icu_data_stream.tellg();
      // Reset to the begining
      icu_data_stream.seekg( 0 );
      if ( icu_data_stream ) {
        // Allocate the memory buffer to contain the file contents
        icu_data_memory =
            new char[sz + decltype( sz )( 1 )];  // Allocate +1 byte that will be set to 0 for sanity/safety
        icu_data_memory[sz] = 0;                 // safety
        // Read the file to the memory buffer
        icu_data_stream.read( icu_data_memory, sz );
        if ( icu_data_stream ) {
          // Initialize ICU with the loaded memory buffer
          udata_setCommonData( icu_data_memory, &status );
          if ( U_SUCCESS( status ) ) {
            icu_init_success = true;
          }
        }
      }
      // In all cases close the stream
      icu_data_stream.close();
    }

    if ( !icu_init_success ) {
      std::cerr << "warning: ICU Initialization Failed: " << status << std::endl;
    }
  }

  if ( icu_init_success ) {
    return 0;
  } else {
    return 1;
  }
}
