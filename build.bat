cd build
cmake ..

cmake --build .
cd test
ctest -C Debug

cd ..
cmake --build . --config Release
cd test
ctest -C Release

@pause