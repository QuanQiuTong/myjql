cd build
cmake ..
cmake --build .
cmake --build . --config Release
cd test
ctest -C Debug
ctest -C Release
@pause