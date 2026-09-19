// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

fn main() {
    println!("cargo:rerun-if-changed=spatial_index.hpp");
    println!("cargo:rerun-if-changed=spatial_index.mm");
    println!("cargo:rerun-if-changed=sedona_metalspatial_c.h");
    println!("cargo:rerun-if-changed=sedona_metalspatial_c.mm");
    println!("cargo:rerun-if-changed=geom_types.hpp");
    println!("cargo:rerun-if-changed=box_intersection.metal");
    println!("cargo:rerun-if-changed=bvh.metal");
    println!("cargo:rerun-if-changed=refine.metal");
    println!("cargo:rerun-if-changed=spatial_hash.metal");

    #[cfg(target_os = "macos")]
    {
        cc::Build::new()
            .cpp(true)
            .std("c++20")
            .flag("-fobjc-arc")
            .include(".")
            .file("spatial_index.mm")
            .file("sedona_metalspatial_c.mm")
            .compile("sedona_metalspatial");

        println!("cargo:rustc-link-lib=framework=Metal");
        println!("cargo:rustc-link-lib=framework=Foundation");
        println!("cargo:rustc-link-lib=framework=MetalKit");
    }
}
