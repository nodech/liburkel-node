{
  "variables": [
    "conditions": [
      ["OS == 'win'", {
        "tls_keyword%": "__declspec(thread)"
      }, {
        "tls_keyword%": "<!(./deps/checks/check_tls.sh)"
      }],
    ]
  ],
  "target_defaults": {
  },
  "targets": [
    {
      "target_name": "liburkel",
      "type": "static_library",
      "includes_dir": [
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
          ]
        }],
        ["OS == 'linux'", {
          "defines": ["_POSIX_C_SOURCE=200112"]
        }]
        ["OS == 'win'", {
          "defines": ["_WIN32_WINNT=0x0501"]
        }],
        ["tls_keyword != 'none'", {
          "defines": ["URKEL_TLS=<(tls_keyword)"]
        }]
      ],
    }
  ]
}
