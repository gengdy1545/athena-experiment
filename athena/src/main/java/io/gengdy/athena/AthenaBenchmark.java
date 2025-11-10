package io.gengdy.athena;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AthenaBenchmark
{
	private static final String CONFIG_FILE_PATH = "config.properties";

	private Region AWS_REGION;
	private String ATHENA_OUTPUT_S3_BUCKET;
	private String QUERY_FILE_PATH;
	private String RESULTS_FILE_PATH;
	private double DOLLARS_PER_TB; // Athena per-TB pricing (in USD)

	public static void main(String[] args)
	{
		try
		{
			new AthenaBenchmark().runTest();
		} catch (Exception e)
		{
			System.err.println("Benchmark failed with exception.");
			e.printStackTrace();
		}
	}

	public void runTest() throws Exception
	{
		loadConfiguration();

		System.out.println("--- Athena Benchmark Test Starting ---");
		AthenaClient athenaClient = null;
		try
		{
			athenaClient = AthenaClient.builder()
					.region(AWS_REGION)
					.build();
		} catch (Exception e)
		{
			System.err.println("Failed to create Athena client.");
			e.printStackTrace();
			return;
		}

		// Thread-safe list to hold query execution info
		List<QueryExecutionInfo> queryExecutionInfos = Collections.synchronizedList(new ArrayList<>());

		List<String> lines = Files.readAllLines(Paths.get(QUERY_FILE_PATH));
		long startTime = System.currentTimeMillis();
		long num = 1;

		System.out.println("--- 1. Starting to submit query stream (total " + lines.size() + " queries) ---");

		for (String line : lines)
		{
			String[] parts = line.split(",", 4);
			if (parts.length < 4)
			{
				System.err.println("Skipping malformed line (expected 4 columns): " + line);
				continue;
			}

			long start = Long.parseLong(parts[0].trim());
			String databaseName = parts[1].trim();
			int query_id = Integer.parseInt(parts[2].trim());
			String query = parts[3].trim();

			while (true)
			{
				long currentTime = System.currentTimeMillis();
				long delay = start - (currentTime - startTime);
				if (delay <= 0)
				{
					break;
				}
				Thread.sleep(Math.min(delay, 50));
			}

			long submissionTime = System.currentTimeMillis();

			QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
					.database(databaseName)
					.build();

			String outputLocation = ATHENA_OUTPUT_S3_BUCKET + databaseName + "/" + query_id + "-" + submissionTime;

			ResultConfiguration resultConfiguration = ResultConfiguration.builder()
					.outputLocation(outputLocation)
					.build();

			StartQueryExecutionRequest startQueryRequest = StartQueryExecutionRequest.builder()
					.queryString(query)
					.queryExecutionContext(queryExecutionContext)
					.resultConfiguration(resultConfiguration)
					.build();

			try
			{
				StartQueryExecutionResponse response = athenaClient.startQueryExecution(startQueryRequest);
				String queryExecutionId = response.queryExecutionId();

				queryExecutionInfos.add(new QueryExecutionInfo(query_id, queryExecutionId, submissionTime, false));
				System.out.println(num + ". submitting query (QueryID: " + query_id + ") to database " + databaseName + "...");
				num += 1;

			} catch (Exception e)
			{
				System.err.println("Failed to submit query (QueryID: " + query_id + ")");
				System.err.println("SQL: " + query);
				e.printStackTrace();
			}
		}

		System.out.println("--- 2. All queries submitted, starting to poll for completion ---");

		num = 1;
		boolean isAllFinished;
		long lastPollTime = System.currentTimeMillis();

		do
		{
			isAllFinished = true;
			boolean workDoneInLoop = false;

			for (QueryExecutionInfo info : queryExecutionInfos)
			{
				if (info.isFinished())
				{
					continue;
				}

				isAllFinished = false;

				GetQueryExecutionRequest statusRequest = GetQueryExecutionRequest.builder()
						.queryExecutionId(info.getTraceToken())
						.build();

				try
				{
					GetQueryExecutionResponse statusResponse = athenaClient.getQueryExecution(statusRequest);
					QueryExecutionStatus status = statusResponse.queryExecution().status();
					QueryExecutionState state = status.state();

					if (state == QueryExecutionState.SUCCEEDED)
					{
						info.setFinished(status.toString());
						workDoneInLoop = true;

						QueryExecutionStatistics stats = statusResponse.queryExecution().statistics();

						double pendingTime = (double) stats.queryQueueTimeInMillis();
						double executionTime = (double) stats.engineExecutionTimeInMillis();
						long dataScannedBytes = stats.dataScannedInBytes();

						// Calculate cost in cents: cost = (dataScannedBytes / 10^12) * DOLLARS_PER_TB * 100
						double costCents = (double)dataScannedBytes / 10_000_000_000.0 * DOLLARS_PER_TB;

						info.setPendingTime(pendingTime);
						info.setExecutionTime(executionTime);
						info.setCostCents(costCents);

						System.out.println("-> " + num + ". completed: " + info.getTraceToken() + " (QueryID: " + info.queryID + ")");
						num += 1;

					} else if (state == QueryExecutionState.FAILED || state == QueryExecutionState.CANCELLED)
					{
						info.setFinished(status.toString());
						workDoneInLoop = true;
						String reason = status.stateChangeReason() != null ? status.stateChangeReason() : "Unknown error";
						System.err.println(num + ". failed: " + info.getTraceToken() + " (QueryID: " + info.queryID + ") - Reason: " + reason);

						info.setPendingTime(-1);
						info.setExecutionTime(-1);
						num += 1;
					}
				} catch (Exception e)
				{
					System.err.println("Failed to poll status for query (QueryID: " + info.queryID + ")");
					e.printStackTrace();
					isAllFinished = false;
				}
			}

			if (isAllFinished)
			{
				break;
			}

			if (System.currentTimeMillis() - lastPollTime > 5000)
			{
				System.out.println("--- Athena Benchmark Polling Status ---");
				System.out.println("Completed " + (num - 1) + " out of " + queryExecutionInfos.size() + " queries.");
				lastPollTime = System.currentTimeMillis();
			}

			if (!workDoneInLoop)
			{
				TimeUnit.SECONDS.sleep(1);
			}

		} while (true);

		System.out.println("--- 3. All queries completed, writing results ---");

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(RESULTS_FILE_PATH)))
		{
			for (QueryExecutionInfo info : queryExecutionInfos)
			{
				writer.write(info.toString());
				writer.newLine();
			}
		} catch (IOException e)
		{
			System.out.println("Cannot write results to file: " + RESULTS_FILE_PATH);
			e.printStackTrace();
		}
		System.out.println("--- Benchmark test completed ---");
		System.out.println("Results written to: " + RESULTS_FILE_PATH);

		if (athenaClient != null)
		{
			athenaClient.close();
		}
	}

	private void loadConfiguration() throws IOException
	{
		Properties prop = new Properties();
		InputStream input = AthenaBenchmark.class.getClassLoader().getResourceAsStream(CONFIG_FILE_PATH);
		if (input == null)
		{
			System.out.println("Unable to find " + CONFIG_FILE_PATH +  " in the classpath.");
			throw new FileNotFoundException("Reourse file not found in classpath: " + CONFIG_FILE_PATH);
		}
		prop.load(input);

		AWS_REGION = Region.of(prop.getProperty("aws.region", "US_EAST_2"));
		ATHENA_OUTPUT_S3_BUCKET = prop.getProperty("athena.output.s3.bucket");
		QUERY_FILE_PATH = prop.getProperty("query.file.path");
		RESULTS_FILE_PATH = prop.getProperty("results.file.path");
		DOLLARS_PER_TB = Double.parseDouble(prop.getProperty("athena.dollars.per.tb", "5.0"));

		if (ATHENA_OUTPUT_S3_BUCKET == null || QUERY_FILE_PATH == null || RESULTS_FILE_PATH == null)
		{
			throw new IllegalArgumentException("Missing required properties in " + CONFIG_FILE_PATH);
		}

		System.out.println("--- Configuration Loaded ---");
		System.out.println("Region: " + AWS_REGION);
		System.out.println("S3 Output Bucket: " + ATHENA_OUTPUT_S3_BUCKET);
		System.out.println("Query File: " + QUERY_FILE_PATH);
		System.out.println("Result File: " + RESULTS_FILE_PATH);
		System.out.println("Pricing: $" + DOLLARS_PER_TB + " per TB");
	}
}