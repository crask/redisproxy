if [ ! -f configure ] ; then
    autoreconf -fi > /dev/null 2>&1
    if [[ $? != 0 ]] ; then
        echo "autoreconf failed"
        exit 1
    fi
fi

if [ ! -f Makefile.release ] ; then
    ./configure --enable-debug=log > /dev/null 2>&1
    if [[ $? != 0 ]] ; then
        echo "configure failed for release"
        exit 1
    fi
    cp Makefile Makefile.release
fi

make --quiet -f Makefile.release > /dev/null  2>&1
if [[ $? != 0 ]] ; then
    echo "build failed for release"
    exit 1
fi
echo "build passed for release"

if [ ! -d build/release ] ; then
    mkdir -p build/release
fi
cp src/nutcracker build/release/

if [ ! -f Makefile.debug ] ; then
    ./configure --enable-debug=full CFLAGS="-g -O0" > /dev/null  2>&1
    if [[ $? != 0 ]] ; then
        echo "configure failed for debug"
        exit 1
    fi
    cp Makefile Makefile.debug
fi

make --quiet -f Makefile.debug > /dev/null  2>&1
if [[ $? != 0 ]] ; then
    echo "build failed for debug"
    exit 1
fi
echo "build passed for debug"

if [ ! -d build/debug ] ; then
    mkdir -p build/debug
fi
cp src/nutcracker build/debug/
