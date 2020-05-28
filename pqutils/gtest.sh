wget https://github.com/google/googletest/archive/release-1.8.0.tar.gz
tar xf release-1.8.0.tar.gz
cd googletest-release-1.8.0
cmake -DBUILD_SHARED_LIBS=ON .
make
#sudo cp -a include/gtest /usr/include
#sudo cp -a libgtest_main.so libgtest.so /usr/lib/
#sudo ldconfig -v | grep gtest

