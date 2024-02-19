module.exports = {
    "extends": [
        "@lune-climate",
    ],
    "parserOptions": {
        "ecmaVersion": 12,
        "sourceType": "module",

        // These are needed for some of the typescript-eslint type-based linting rules
        "tsconfigRootDir": __dirname,
        "project": ["./tsconfig.json"],
    },
    rules: {
      'simple-import-sort/imports': [
        'error',
        {
            // Following https://github.com/lydell/eslint-plugin-simple-import-sort/blob/31dc8854127a801e1cc6f1516c23854ea11b311f/examples/.eslintrc.js#L74=
            groups: [
            // Node.js builtins.
            [`^(${require("module").builtinModules.join("|")})(/|$)`],
            // Third party packages.
            ["^@?\\w"],
            // Internal packages. This ensures that our own imports are grouped together.
            ["^src/"],
            // Side effect imports.
            ["^\\u0000"],
            ],
        },
      ],
      'complexity': ['error', { 'max': 14 }],

      'no-restricted-imports': [
        'error',
        {
          patterns: [
            {
              group: [
                './*',
                '../*',
              ], message: 'Relative imports disallowed. Import from src/.'
            },
            {
              group: [
                // This is a bit tricky because no-restricted-imports patterns are Gitignore-like
                // patterns. That means we can't use regular expressions and features like
                // "match this import path only when the suffix doesn't match .js".
                //
                // So the way we restrict only extension-less imports from src/ is as follows:
                // 1. Restrict all src/ imports regardless of the presence of the extension.
                'src/**',
                // 2. Un-restrict imports from *directories*. This is artificial (we don't actually
                // import from directories directly) but it's required for point 3 to work. The
                // reason for that is we can't un-restrict files without first un-restricting
                // their parent directories.
                //
                // See https://github.com/eslint/eslint/issues/16747#issuecomment-1371548988
                '!src/**/',
                // 3. Finally we can un-restrict the "good" src/ imports. Only the bad ones
                // should be left (and trigger an error).
                '!src/**/*.js',
              ], message: 'Internal import from src/ need to have the .js extension present'
            },
          ],
        },
      ],
    }
};
