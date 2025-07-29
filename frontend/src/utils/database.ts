/**
 * Utility function to extract database name from complex path
 * @param dbPath - Database path that might include branch info (e.g., "test_db/local/branch/direct-test-fix")
 * @returns Just the database name (e.g., "test_db") or null if invalid
 */
export const extractDatabaseName = (dbPath: string | null | undefined): string | null => {
  if (!dbPath) return null;
  // Extract just the database name from paths like "test_db/local/branch/direct-test-fix"
  return dbPath.split('/')[0];
};