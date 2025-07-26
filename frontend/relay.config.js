module.exports = {
  src: './src',
  language: 'typescript',
  schema: './data/schema.graphql',
  artifactDirectory: './src/__generated__',
  exclude: ['**/node_modules/**', '**/__mocks__/**', '**/__generated__/**'],
  extensions: ['ts', 'tsx'],
  watchman: false,
  customScalars: {
    DateTime: 'string',
    Date: 'string',
    JSON: 'any',
    UUID: 'string',
  },
  persistConfig: {
    file: './queryMap.json',
  },
};