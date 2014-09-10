export PATH=/opt/compiler/gcc-4.8.2/bin:$PATH
autoreconf -fvi
./configure --enable-debug=log
make
rm -rf output
mkdir -p output/src
cp src/nutcracker output/src || true
