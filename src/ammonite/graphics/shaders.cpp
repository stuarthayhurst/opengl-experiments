#include <algorithm>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <GL/glew.h>

#include "../core/fileManager.hpp"
#include "../core/threadManager.hpp"

#include "../utils/logging.hpp"
#include "internal/internalExtensions.hpp"
#include "internal/internalShaderValidator.hpp"

namespace ammonite {
  //Static helper functions and anonymous data
  namespace {
    namespace {
      //Set by updateCacheSupport(), when GLEW loads
      bool isBinaryCacheSupported = false;

      //Data required by cache worker
      struct CacheWorkerData {
        int shaderCount;
        std::string* shaderPaths;
        std::string cacheFilePath;
        GLenum binaryFormat;
        int binaryLength;
        char* binaryData;
      };
    }

    static void deleteCacheFile(std::string cacheFilePath) {
      //Delete the cache file
      ammonite::utils::status << "Clearing '" << cacheFilePath << "'" << std::endl;
      ammonite::files::internal::deleteFile(cacheFilePath);
    }

    //Thread pool work to cache a program
    static void doCacheWork(void* userPtr) {
      CacheWorkerData* data = (CacheWorkerData*)userPtr;

      //Generate cache info to write
      std::string extraData;
      for (int i = 0; i < data->shaderCount; i++) {
        long long int filesize = 0, modificationTime = 0;
        ammonite::files::internal::getFileMetadata(data->shaderPaths[i], &filesize,
                                                   &modificationTime);

        extraData.append("input;" + data->shaderPaths[i]);
        extraData.append(";" + std::to_string(filesize));
        extraData.append(";" + std::to_string(modificationTime) + "\n");
      }

      extraData.append(std::to_string(data->binaryFormat) + "\n");
      extraData.append(std::to_string(data->binaryLength) + "\n");

      int extraLength = extraData.length();
      char* fileData = new char[extraLength + data->binaryLength];

      //Write the cache info and binary data to the buffer
      extraData.copy(fileData, extraLength, 0);
      std::memcpy(fileData + extraLength, data->binaryData, data->binaryLength);

      //Write the cache info and data to the cache file
      if (!ammonite::files::internal::writeFile(data->cacheFilePath, (unsigned char*)fileData,
                                                extraLength + data->binaryLength)) {
        ammonite::utils::warning << "Failed to cache '" << data->cacheFilePath << "'" << std::endl;
        deleteCacheFile(data->cacheFilePath);
      }

      delete [] fileData;
      delete [] data->binaryData;
      delete [] data->shaderPaths;
      delete data;
    }

    static void cacheProgram(const GLuint programId, std::string* shaderPaths, int shaderCount,
                             std::string* cacheFilePath) {
      CacheWorkerData* data = new CacheWorkerData;
      data->shaderCount = shaderCount;

      data->cacheFilePath = *cacheFilePath;
      ammonite::utils::status << "Caching '" << data->cacheFilePath << "'" << std::endl;

      //Get binary length of linked program
      glGetProgramiv(programId, GL_PROGRAM_BINARY_LENGTH, &data->binaryLength);
      if (data->binaryLength == 0) {
        ammonite::utils::warning << "Failed to cache '" << data->cacheFilePath << "'" << std::endl;
        delete data;
        return;
      }

      //Get binary format and binary data
      int actualBytes = 0;
      data->binaryData = new char[data->binaryLength];
      glGetProgramBinary(programId, data->binaryLength, &actualBytes, &data->binaryFormat,
                         data->binaryData);
      if (actualBytes != data->binaryLength) {
        ammonite::utils::warning << "Program length doesn't match expected length (ID " \
                                 << programId << ")" << std::endl;
        delete [] data->binaryData;
        delete data;
        return;
      }

      //Pack data for writing program cache
      data->shaderPaths = new std::string[shaderCount];
      for (int i = 0; i < shaderCount; i++) {
        data->shaderPaths[i] = shaderPaths[i];
      }

      ammonite::thread::internal::submitWork(doCacheWork, data, nullptr);
    }

    static bool checkObject(GLuint objectId, const char* actionString, GLenum statusEnum,
                            void (*objectQuery)(GLuint, GLenum, GLint*),
                            void (*objectLog)(GLuint, GLsizei, GLsizei*, GLchar*)) {
      //Test whether the program linked
      GLint success = GL_FALSE;
      objectQuery(objectId, statusEnum, &success);

      //If the program linked successfully, return
      if (success == GL_TRUE) {
        return true;
      }

      //Get length of a log, if available
      GLsizei maxLength = 0;
      objectQuery(objectId, GL_INFO_LOG_LENGTH, &maxLength);
      if (maxLength == 0) {
        ammonite::utils::warning << "Failed to " << actionString << " (ID " << objectId \
                                 << "), no log available" << std::endl;
        return false;
      }

      /*
       - Fetch and print the log
       - The extra byte isn't strictly required, but some drivers are buggy
      */
      GLchar* errorLogBuffer = new GLchar[maxLength + 1];
      objectLog(objectId, maxLength, nullptr, errorLogBuffer);
      errorLogBuffer[maxLength] = '\0';
      ammonite::utils::warning << "Failed to " << actionString << " (ID " << objectId \
                               << "):\n" << (char*)errorLogBuffer << std::endl;

      delete [] errorLogBuffer;
      return false;
    }

    static bool checkProgram(GLuint programId) {
      return checkObject(programId, "link shader program", GL_LINK_STATUS,
                         glGetProgramiv, glGetProgramInfoLog);
    }

    static bool checkShader(GLuint shaderId) {
      return checkObject(shaderId, "compile shader stage", GL_COMPILE_STATUS,
                         glGetShaderiv, glGetShaderInfoLog);
    }

    static GLenum attemptIdentifyShaderType(std::string shaderPath) {
      std::map<std::string, GLenum> shaderMatches = {
        {"vert", GL_VERTEX_SHADER},
        {"frag", GL_FRAGMENT_SHADER},
        {"geom", GL_GEOMETRY_SHADER},
        {"tessc", GL_TESS_CONTROL_SHADER}, {"control", GL_TESS_CONTROL_SHADER},
        {"tesse", GL_TESS_EVALUATION_SHADER}, {"eval", GL_TESS_EVALUATION_SHADER},
        {"compute", GL_COMPUTE_SHADER}
      };

      std::string lowerShaderPath;
      for (unsigned int i = 0; i < shaderPath.size(); i++) {
        lowerShaderPath += std::tolower(shaderPath[i]);
      }

      //Try and match the filename to a supported shader
      for (auto it = shaderMatches.begin(); it != shaderMatches.end(); it++) {
        if (lowerShaderPath.contains(it->first)) {
          return it->second;
        }
      }

      return GL_FALSE;
    }
  }

  //Shader compilation and cache functions, local to this file
  namespace {
    //Take shader source code, compile it and load it
    static int loadShader(std::string shaderPath, const GLenum shaderType, bool* externalSuccess) {
      //Create the shader
      GLuint shaderId = glCreateShader(shaderType);

      //Read the shader's source code
      std::size_t shaderCodeSize;
      char* shaderCodePtr = (char*)ammonite::files::internal::loadFile(shaderPath,
                                                                       &shaderCodeSize);
      if (shaderCodePtr == nullptr) {
        ammonite::utils::warning << "Failed to open '" << shaderPath << "'" << std::endl;
        *externalSuccess = false;
        return -1;
      }

      glShaderSource(shaderId, 1, &shaderCodePtr, (int*)&shaderCodeSize);
      glCompileShader(shaderId);
      delete [] shaderCodePtr;

      //Check whether the shader compiled, log if relevant
      if (!checkShader(shaderId)) {
        glDeleteShader(shaderId);
        *externalSuccess = false;
        return -1;
      }

      return shaderId;
    }

    //Take multiple shader objects and create a program
    static int createProgramObject(GLuint* shaderIds, const int shaderCount,
                                   bool* externalSuccess) {
      //Create the program
      GLuint programId = glCreateProgram();

      //Attach and link all passed shader ids
      for (int i = 0; i < shaderCount; i++) {
        glAttachShader(programId, shaderIds[i]);
      }
      glLinkProgram(programId);

      //Detach and remove all passed shader ids
      for (int i = 0; i < shaderCount; i++) {
        glDetachShader(programId, shaderIds[i]);
        glDeleteShader(shaderIds[i]);
      }

      if (!checkProgram(programId)) {
        glDeleteProgram(programId);
        *externalSuccess = false;
        return -1;
      }

      return programId;
    }

    //Create a program from shader source with loadShader() and createProgramObject()
    static int createProgramUncached(std::string* shaderPaths, GLenum* shaderTypes,
                                     int shaderCount, bool* externalSuccess) {
      //Since cache wasn't available, generate fresh shaders
      GLuint* shaderIds = new GLuint[shaderCount];
      bool hasCreatedShaders = true;
      for (int i = 0; i < shaderCount; i++) {
        shaderIds[i] = loadShader(shaderPaths[i], shaderTypes[i], externalSuccess);
        if (shaderIds[i] == unsigned(-1)) {
          hasCreatedShaders = false;
          break;
        }
      }

      //Create the program from the shaders
      GLuint programId;
      bool hasCreatedProgram = false;
      if (hasCreatedShaders) {
        hasCreatedProgram = true;
        programId = createProgramObject(shaderIds, shaderCount, &hasCreatedProgram);
      }

      //Clean up
      if (!hasCreatedProgram || !hasCreatedShaders) {
        *externalSuccess = false;
        for (int i = 0; i < shaderCount; i++) {
          if (shaderIds[shaderCount] != unsigned(-1)) {
            glDeleteShader(shaderIds[shaderCount]);
          }
        }
        delete [] shaderIds;
        return -1;
      }
      delete [] shaderIds;

      return programId;
    }

    //Attempt to use a cached program or hand off to createProgramUncached()
    static int createProgramCached(std::string* shaderPaths, GLenum* shaderTypes,
                                   int shaderCount, bool* externalSuccess) {
      //Check for OpenGL and engine cache support
      bool isCacheSupported = ammonite::files::internal::getCacheEnabled();
      isCacheSupported = isCacheSupported && isBinaryCacheSupported;

      //Try and fetch the cache, then try and load it into a program
      int programId;
      std::string cacheFilePath;
      if (isCacheSupported) {
        std::size_t cacheDataSize;
        AmmoniteEnum cacheState;

        //Attempt to load the cached program
        ammonite::shaders::internal::CacheInfo cacheInfo = {shaderCount, shaderPaths, 0, 0};
        cacheFilePath = ammonite::files::internal::getCachedFilePath(shaderPaths,
                                                                                 shaderCount);
        unsigned char* cacheData = ammonite::files::internal::getCachedFile(cacheFilePath,
          ammonite::shaders::internal::validateCache, &cacheDataSize, &cacheState, &cacheInfo);
        unsigned char* dataStart = cacheData + cacheDataSize - cacheInfo.binaryLength;

        if (cacheState == AMMONITE_CACHE_HIT) {
          //Load the cached binary data into a program
          programId = glCreateProgram();
          glProgramBinary(programId, cacheInfo.binaryFormat, dataStart, cacheInfo.binaryLength);
          delete [] cacheData;

          //Return the program ID, unless the cache was faulty, then delete and carry on
          if (checkProgram(programId)) {
            return programId;
          } else {
            ammonite::utils::warning << "Failed to process '" << cacheFilePath \
                                     << "'" << std::endl;
            glDeleteProgram(programId);
            deleteCacheFile(cacheFilePath);
          }
        }
      }

      //Cache wasn't useable, compile a fresh program
      programId = createProgramUncached(shaderPaths, shaderTypes, shaderCount, externalSuccess);

      //Cache the binary if enabled
      if (isCacheSupported && programId != -1) {
        cacheProgram(programId, shaderPaths, shaderCount, &cacheFilePath);
      }

      return programId;
    }
  }

  //Internally exposed functions
  namespace shaders {
    namespace internal {
      //Set isBinaryCacheSupported according to OpenGL support
      void updateCacheSupport() {
        //Get number of supported formats
        GLint numBinaryFormats = 0;
        glGetIntegerv(GL_NUM_PROGRAM_BINARY_FORMATS, &numBinaryFormats);

        //Check support for collecting the program binary
        isBinaryCacheSupported = true;
        if (!graphics::internal::checkExtension("GL_ARB_get_program_binary", "GL_VERSION_4_1")) {
          ammonite::utils::warning << "Program caching unsupported" << std::endl;
          isBinaryCacheSupported = false;
        } else if (numBinaryFormats < 1) {
          ammonite::utils::warning << "Program caching unsupported (no supported formats)" \
                                   << std::endl;
          isBinaryCacheSupported = false;
        }
      }

      /*
       - Take an array of shader paths, create a program and return the ID
         - Shaders with unidentifiable types will be ignored
         - The order of shaders may be changed without re-caching
       - If possible, load and store a cache
       - Writes 'false' to externalSuccess on failure and returns -1
      */
      int createProgram(std::string* inputShaderPaths, int inputShaderCount,
                        bool* externalSuccess) {
        //Convert file extensions to shader types
        std::map<std::string, GLenum> shaderExtensions = {
          {".vert", GL_VERTEX_SHADER}, {".vs", GL_VERTEX_SHADER},
          {".frag", GL_FRAGMENT_SHADER}, {".fs", GL_FRAGMENT_SHADER},
          {".geom", GL_GEOMETRY_SHADER}, {".gs", GL_GEOMETRY_SHADER},
          {".tessc", GL_TESS_CONTROL_SHADER}, {".tsc", GL_TESS_CONTROL_SHADER},
          {".tesse", GL_TESS_EVALUATION_SHADER}, {".tes", GL_TESS_EVALUATION_SHADER},
          {".comp", GL_COMPUTE_SHADER}, {".cs", GL_COMPUTE_SHADER},
          {".glsl", GL_FALSE} //Detect generic shaders, attempt to identify further
        };

        //Find all shaders
        std::vector<std::string> shaderPaths(0);
        std::vector<GLenum> shaderTypes(0);
        for (int i = 0; i < inputShaderCount; i++) {
          std::filesystem::path filePath{inputShaderPaths[i]};
          std::string extension = filePath.extension();

          if (shaderExtensions.contains(extension)) {
            GLenum shaderType = shaderExtensions[extension];

            //Shader can't be identified through extension, use filename
            if (shaderType == GL_FALSE) {
              GLenum newType = attemptIdentifyShaderType(inputShaderPaths[i]);
              if (newType == GL_FALSE) {
                ammonite::utils::warning << "Couldn't identify type of shader '"
                        << inputShaderPaths[i] << "'" << std::endl;
                continue;
              }

              //Found a type, so use it
              shaderType = newType;
            }

            //Check for compute shader support if needed
            if (shaderType == GL_COMPUTE_SHADER) {
              if (!graphics::internal::checkExtension("GL_ARB_compute_shader",
                                                      "GL_VERSION_4_3")) {
                ammonite::utils::warning << "Compute shaders unsupported" << std::endl;
                continue;
              }
            }

            //Check for tessellation shader support if needed
            if (shaderType == GL_TESS_CONTROL_SHADER or shaderType == GL_TESS_EVALUATION_SHADER) {
              if (!graphics::internal::checkExtension("GL_ARB_tessellation_shader",
                                                      "GL_VERSION_4_0")) {
                ammonite::utils::warning << "Tessellation shaders unsupported" << std::endl;
                continue;
              }
            }

            shaderPaths.push_back(std::string(filePath));
            shaderTypes.push_back(shaderType);
          }
        }

        //Ensure shaders don't get rebuilt due to a file order change
        std::sort(shaderPaths.begin(), shaderPaths.end());

        //Create the program and return the ID
        return createProgramCached(&shaderPaths[0], &shaderTypes[0], shaderPaths.size(),
                                   externalSuccess);
      }

      /*
       - Create a program from shaders in a directory and return the ID
       - If possible, load and store a cache
       - Writes 'false' to externalSuccess on failure and returns -1
      */
      int loadDirectory(const char* directoryPath, bool* externalSuccess) {
        //Create filesystem directory iterator
        std::filesystem::directory_iterator it;
        try {
          const std::filesystem::path shaderDir{directoryPath};
          it = std::filesystem::directory_iterator{shaderDir};
        } catch (const std::filesystem::filesystem_error&) {
          *externalSuccess = false;
          ammonite::utils::warning << "Failed to load '" << directoryPath << "'" << std::endl;
          return -1;
        }

        //Find files to send to next stage
        std::vector<std::string> shaderPaths(0);
        for (auto const& fileName : it) {
          std::filesystem::path filePath{fileName};
          shaderPaths.push_back(std::string(filePath));
        }

        //Create the program and return the ID
        return createProgram(&shaderPaths[0], shaderPaths.size(), externalSuccess);
      }
    }
  }
}
