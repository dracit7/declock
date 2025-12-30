#!/bin/bash

script_dir=$(dirname "${0}")
root=$script_dir/..

if [ ! -f vendor/junction/install/lib/libjunction.a ]; then
	echo "+++ Building Junction..."
	cd $root/vendor/junction && mkdir -p build && mkdir -p install && cd build
	cmake -DCMAKE_INSTALL_PREFIX="../install" ..
	make install -j

	echo "+++ Building Turf..."
	cd $root/vendor/junction && mkdir -p build && cd build
	cmake ..
	make -j
fi

if [ ! -f vendor/r2/libr2.a ]; then
	cd $root/vendor/r2
	mkdir -p build && cd build/
	cmake .. && make boost && make r2
	mv libr2.a ../
fi
