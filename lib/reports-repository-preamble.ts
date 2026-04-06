/** Re-exports — use `reports-repository-header` for new code. */
export {
  REPORTS_REPOSITORY_PREAMBLE_LINE_COUNT,
  stripReportsRepositoryPreamble,
  findReportsRepositoryHeaderLineIndex,
  sliceCsvFromHeaderLine,
  fileNameSuggestsReportsRepository,
  contentSuggestsReportsRepositorySample,
  lineLooksLikeReportsRepositoryHeader,
  type ReportsRepoHeaderDetection,
} from "./reports-repository-header";
