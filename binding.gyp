{
  "variables": {
    "conditions": [
      ["OS == 'win'", {
        "tls_keyword%": "__declspec(thread)"
      }, {
        "tls_keyword%": "<!(./deps/checks/check_tls.sh)"
      }],
    ]
  },
  "targets": [
    {
      "target_name": "liburkel",
      "type": "static_library",
      "sources": [
        "./deps/liburkel/src/bits.c",
        "./deps/liburkel/src/blake2b.c",
        "./deps/liburkel/src/internal.c",
        "./deps/liburkel/src/io.c",
        "./deps/liburkel/src/nodes.c",
        "./deps/liburkel/src/proof.c",
        "./deps/liburkel/src/store.c",
        "./deps/liburkel/src/tree.c",
        "./deps/liburkel/src/util.c"
      ],
      "include_dirs": [
        "./deps/liburkel/include"
      ],
      "direct_dependent_settings": {
        "include_dirs": [
          "./deps/liburkel/include"
        ]
      },
      "conditions": [
        ["OS != 'mac' and OS != 'win'", {
          "cflags": [
            "-fvisibility=hidden",
            "-pedantic",
            "-Wall",
            "-Wextra",
            "-Wextra",
            "-Wcast-align",
            "-Wno-implicit-fallthrough",
            "-Wno-long-long",
            "-Wno-overlength-strings",
            "-Wshadow",
          ]
        }],
        ["OS == 'mac'", {
          "xcode_settings": {
            "GCC_SYMBOLS_PRIVATE_EXTERN": "YES",
            "MACOSX_DEPLOYMENT_TARGET": "10.7",
            "WARNING_CFLAGS": [
              "-pedantic",
              "-Wall",
              "-Wextra",
              "-Wextra",
              "-Wcast-align",
              "-Wno-implicit-fallthrough",
              "-Wno-long-long",
              "-Wno-overlength-strings",
              "-Wshadow",
            ]
          }
        }],
        ["OS == 'win'", {
          "msvs_disabled_warnings=": [
            4146, # negation of unsigned integer
            4244, # implicit integer demotion
            4267, # implicit size_t demotion
            4334, # implicit 32->64 bit shift
            4996, # deprications
          ],
        }],
        # ["OS == 'win'", {
        #   "defines": ["_WIN32_WINNT=0x0501"],
        #   "sources": [
        #     "./deps/liburkel/src/io_win.c"
        #   ]
        # }],
        # ["OS != 'win'", {
        #   "sources": [
        #     "./deps/liburkel/src/io_posix.c"
        #   ]
        # }],
        ["OS == 'linux'", {
          "defines": ["_POSIX_C_SOURCE=200112"]
        }],
        ["tls_keyword != 'none'", {
          "defines": ["URKEL_TLS=<(tls_keyword)"]
        }]
      ],
    },
    {
      "target_name": "nurkel",
      "dependencies": [
        "liburkel",
      ],
      "sources": [
        "./src/nurkel.c"
      ],
      "conditions": [
        ["OS != 'mac' and OS != 'win'", {
          "cflags": [
            "-Wcast-align",
            "-Wshadow"
          ]
        }],
        ["OS == 'mac'", {
          "xcode_settings": {
            "WARNING_CFLAGS": [
              "-Wcast-align",
              "-Wshadow"
            ]
          }
        }],
        ["OS == 'win'", {
          "msvs_disabled_warnings=": [
            4244, # implicit integer demotion
            4267  # implicit size_t demotion
          ]
        }],
      ]
    }
  ]
}
