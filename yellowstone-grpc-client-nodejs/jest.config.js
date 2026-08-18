module.exports = {
  clearMocks: true,
  moduleDirectories: ["node_modules", "dist"],
  setupFiles: ["dotenv/config"],
  transform: {
    "^.+\\.tsx?$": [
      "@swc/jest",
      {
        jsc: {
          parser: {
            syntax: "typescript",
          },
          target: "es2022",
        },
        module: {
          type: "commonjs",
        },
      },
    ],
  },
  verbose: true,
};
