#'
#' @export
runCohortCharacterization <- function(packageName = NULL,
                                      cohortToCreateFile = "settings/CohortsToCreate.csv",
                                      baseUrl = NULL,
                                      connectionDetails = NULL,
                                      connection = NULL,
                                      cdmDatabaseSchema,
                                      oracleTempSchema = NULL,
                                      cohortDatabaseSchema,
                                      cohortTable = "cohort",
                                      cohortIds = NULL,
                                      outputFolder,
                                      exportFolder = file.path(outputFolder,"diagnosticsExport"),
                                      databaseId,
                                      databaseName = databaseId,
                                      databaseDescription = "",
                                      baselineCovariateSettings,
                                      postIndexCovariateSettings,
                                      minCellCount = 5) {
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (is.null(baselineCovariateSettings) && is.null(postIndexCovariateSettings)) {
    stop("This function requires specifying a set of baseline covariate settings and/or post index covariate settings")
  }
  
  # Hacked from CohortDiagnostics::runCohortDiagnostics
  # Line 297: if (runCohortCharacterization) {....}
  # CohortDiagnostics::runCohortDiagnostics Code Start -----
  ParallelLogger::logInfo("Creating cohort characterizations")
  
  cohorts <- loadCohortsFromPackage(packageName = packageName,
                                    cohortToCreateFile = cohortToCreateFile,
                                    cohortIds = cohortIds)
  
  ParallelLogger::logInfo("Counting cohorts")
  counts <- getCohortCounts(connection = connection,
                            cohortDatabaseSchema = cohortDatabaseSchema,
                            cohortTable = cohortTable,
                            cohortIds = cohorts$cohortId)
  
  runCohortCharacterization <- function(row, covariateSettings) {
    ParallelLogger::logInfo("- Creating characterization for cohort ", row$cohortName)
    data <- CohortDiagnostics::getCohortCharacteristics(connection = connection,
                                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                                        cohortTable = cohortTable,
                                                        cohortId = row$cohortId,
                                                        covariateSettings = covariateSettings)
    if (nrow(data) > 0) {
      data$cohortId <- row$cohortId
    }
    return(data)
  }
  
  data <- data.frame()
  if (!is.null(baselineCovariateSettings)) {
    ParallelLogger::logInfo("Baseline covariates")
    baselineData <- lapply(split(cohorts, cohorts$cohortId), runCohortCharacterization, covariateSettings = baselineCovariateSettings)
    baselineData <- do.call(rbind, baselineData)
    data <- baselineData
  }
  if (!is.null(postIndexCovariateSettings)) {
    ParallelLogger::logInfo("Post-index covariates")
    postIndexData <- lapply(split(cohorts, cohorts$cohortId), runCohortCharacterization, covariateSettings = postIndexCovariateSettings)
    postIndexData <- do.call(rbind, postIndexData)
    postIndexData$covariateId <- postIndexData$covariateId * -1
    if (length(data) == 0) {
      data <- postIndexData
    } else {
      data <- rbind(data, postIndexData)
    }
  }
  
  # Drop covariates with mean = 0 after rounding to 3 digits:
  data <- data[round(data$mean, 3) != 0, ]
  covariates <- unique(data[, c("covariateId", "covariateName", "analysisId")])
  colnames(covariates)[[3]] <- "covariateAnalysisId"
  writeToCsv(covariates, file.path(exportFolder, "covariate.csv"))
  data$covariateName <- NULL
  data$analysisId <- NULL
  if (nrow(data) > 0) {
    data$databaseId <- databaseId
    data <- merge(data, counts[, c("cohortId", "cohortEntries")])
    data <- enforceMinCellValue(data, "mean", minCellCount/data$cohortEntries)
    data$sd[data$mean < 0] <- NA
    data$cohortEntries <- NULL
    data$mean <- round(data$mean, 3)
    data$sd <- round(data$sd, 3)
  }
  writeToCsv(data, file.path(exportFolder, "covariate_value.csv"))
  # CohortDiagnostics::runCohortDiagnostics Code End -----
}

# Copied from CohortDiagnostics RunDiagnostics.R
loadCohortsFromPackage <- function(packageName, cohortToCreateFile, cohortIds) {
  pathToCsv <- system.file(cohortToCreateFile, package = packageName)
  cohorts <- readr::read_csv(pathToCsv, col_types = readr::cols())
  cohorts$atlasId <- NULL
  if (!is.null(cohortIds)) {
    cohorts <- cohorts[cohorts$cohortId %in% cohortIds, ]
  }
  if ("atlasName" %in% colnames(cohorts)) {
    cohorts <- dplyr::rename(cohorts, cohortName = "name", cohortFullName = "atlasName")
  } else {
    cohorts <- dplyr::rename(cohorts, cohortName = "name", cohortFullName = "fullName")
  }
  
  getSql <- function(name) {
    pathToSql <- system.file("sql", "sql_server", paste0(name, ".sql"), package = packageName)
    sql <- readChar(pathToSql, file.info(pathToSql)$size)
    return(sql)
  }
  cohorts$sql <- sapply(cohorts$cohortName, getSql)
  getJson <- function(name) {
    pathToJson <- system.file("cohorts", paste0(name, ".json"), package = packageName)
    json <- readChar(pathToJson, file.info(pathToJson)$size)
    return(json)
  }
  cohorts$json <- sapply(cohorts$cohortName, getJson)
  return(cohorts)
}


# Copied from CohortDiagnostics RunDiagnostics.R
writeToCsv <- function(data, fileName) {
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  readr::write_csv(data, fileName)
}

# Copied from CohortDiagnostics RunDiagnostics.R
enforceMinCellValue <- function(data, fieldName, minValues, silent = FALSE) {
  toCensor <- !is.na(data[, fieldName]) & data[, fieldName] < minValues & data[, fieldName] != 0
  if (!silent) {
    percent <- round(100 * sum(toCensor)/nrow(data), 1)
    ParallelLogger::logInfo("   censoring ",
                            sum(toCensor),
                            " values (",
                            percent,
                            "%) from ",
                            fieldName,
                            " because value below minimum")
  }
  if (length(minValues) == 1) {
    data[toCensor, fieldName] <- -minValues
  } else {
    data[toCensor, fieldName] <- -minValues[toCensor]
  }
  return(data)
}

# Copied from CohortDiagnostics CohortLevelDiagnostics.R
getCohortCounts <- function(connectionDetails = NULL,
                            connection = NULL,
                            cohortDatabaseSchema,
                            cohortTable = "cohort",
                            cohortIds = c()) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CohortCounts.sql",
                                           packageName = "CohortDiagnostics",
                                           dbms = connection@dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cohort_ids = cohortIds)
  counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Counting cohorts took",
                                signif(delta, 3),
                                attr(delta, "units")))
  return(counts)
  
}