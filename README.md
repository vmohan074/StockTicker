# StockTicker
StockTicker


# Alert Triage Agent Core - Deep Technical Architecture

## Table of Contents

1. [Agent Orchestrator Deep Dive](#agent-orchestrator-deep-dive)
2. [Semantic Kernel Implementation](#semantic-kernel-implementation)
3. [Redis Storage Patterns](#redis-storage-patterns)
4. [Decision Engine Algorithms](#decision-engine-algorithms)
5. [Integration Architecture](#integration-architecture)
6. [Performance & Scaling](#performance--scaling)

---

## Agent Orchestrator Deep Dive

### Architecture Pattern

The Agent Orchestrator follows the **Supervisor Pattern** where it manages multiple worker agents and coordinates their activities. It implements the `AbstractManager` pattern from OrchestrationServer but extends it for AI-driven workflows.

### Core Responsibilities

```csharp
public class AlertTriageOrchestrator : AbstractManager
{
    private readonly IKernel _semanticKernel;
    private readonly IConnectionMultiplexer _redis;
    private readonly IRepositoryFactory _repositoryFactory;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _concurrencyLimiter;
    private readonly CancellationTokenSource _cancellationTokenSource;

    // Configuration
    private readonly int _maxConcurrentAlerts = 50;
    private readonly TimeSpan _sessionTimeout = TimeSpan.FromHours(1);
    private readonly double _autoApplyConfidenceThreshold = 0.85;

    public AlertTriageOrchestrator(
        IUnityContainer container,
        IRepositoryFactory factory)
    {
        _repositoryFactory = factory;
        _semanticKernel = container.Resolve<IKernel>();
        _redis = container.Resolve<IConnectionMultiplexer>();
        _logger = Logger.Get(typeof(AlertTriageOrchestrator));
        _concurrencyLimiter = new SemaphoreSlim(_maxConcurrentAlerts);
        _cancellationTokenSource = new CancellationTokenSource();
    }

    public override async Task RunAsync()
    {
        State = ManagerState.Starting;
        _logger.Info("Alert Triage Orchestrator starting...");

        // Initialize Redis connections
        await InitializeRedisAsync();

        // Warm up caches
        await WarmupCachesAsync();

        State = ManagerState.Running;

        while (!_cancellationTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                // Poll for pending alerts
                var pendingAlerts = await GetPendingAlertsAsync();

                if (pendingAlerts.Any())
                {
                    _logger.Info($"Processing {pendingAlerts.Count} pending alerts");

                    // Process in parallel with concurrency limit
                    await ProcessAlertsInParallelAsync(pendingAlerts);
                }
                else
                {
                    // No work, sleep before next poll
                    await Task.Delay(TimeSpan.FromSeconds(5),
                        _cancellationTokenSource.Token);
                }
            }
            catch (Exception ex)
            {
                _logger.Error("Error in orchestration loop",
                    new Dictionary<string, object> { {"exception", ex} });
                await Task.Delay(TimeSpan.FromSeconds(10));
            }
        }

        State = ManagerState.Stopped;
    }

    private async Task ProcessAlertsInParallelAsync(
        List<Alert> alerts)
    {
        var tasks = alerts.Select(async alert =>
        {
            // Respect concurrency limit
            await _concurrencyLimiter.WaitAsync();
            try
            {
                await ProcessSingleAlertAsync(alert);
            }
            finally
            {
                _concurrencyLimiter.Release();
            }
        });

        await Task.WhenAll(tasks);
    }

    private async Task ProcessSingleAlertAsync(Alert alert)
    {
        var sessionId = Guid.NewGuid().ToString();
        var stopwatch = Stopwatch.StartNew();

        try
        {
            // Create session in Redis
            await CreateSessionAsync(sessionId, alert);

            // Build context for Semantic Kernel
            var context = _semanticKernel.CreateNewContext();
            context.Variables["alert_id"] = alert.Id.ToString();
            context.Variables["alert_data"] = JsonConvert.SerializeObject(alert);
            context.Variables["session_id"] = sessionId;

            // Execute the triage plan
            var plan = await CreateTriagePlanAsync(alert);
            var result = await plan.InvokeAsync(context);

            // Extract recommendation
            var recommendation = JsonConvert.DeserializeObject<DispositionRecommendation>(
                result.Result);

            // Apply or queue for review
            if (recommendation.Confidence >= _autoApplyConfidenceThreshold)
            {
                await ApplyDispositionAsync(alert, recommendation);
                _logger.Info($"Auto-applied disposition for alert {alert.Id}",
                    new Dictionary<string, object>
                    {
                        {"disposition", recommendation.Disposition},
                        {"confidence", recommendation.Confidence}
                    });
            }
            else
            {
                await QueueForAnalystReviewAsync(alert, recommendation);
                _logger.Info($"Queued alert {alert.Id} for analyst review",
                    new Dictionary<string, object>
                    {
                        {"confidence", recommendation.Confidence}
                    });
            }

            // Store decision history in Redis
            await StoreDecisionHistoryAsync(alert, recommendation);

            // Record metrics
            await RecordMetricsAsync(alert, recommendation, stopwatch.Elapsed);
        }
        catch (Exception ex)
        {
            _logger.Error($"Failed to process alert {alert.Id}",
                new Dictionary<string, object>
                {
                    {"exception", ex},
                    {"alert_id", alert.Id}
                });

            // Move to error queue
            await HandleProcessingErrorAsync(alert, ex);
        }
        finally
        {
            // Cleanup session (or let TTL handle it)
            await CleanupSessionAsync(sessionId);
        }
    }
}
```

### Session Management in Detail

**Session Lifecycle:**

```csharp
private async Task CreateSessionAsync(string sessionId, Alert alert)
{
    var db = _redis.GetDatabase();
    var sessionKey = $"session:{sessionId}";

    // Use Redis Hash for structured session data
    var sessionData = new HashEntry[]
    {
        new HashEntry("alert_id", alert.Id),
        new HashEntry("state", "initialized"),
        new HashEntry("started_at", DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
        new HashEntry("alert_type", alert.Type),
        new HashEntry("amount", alert.Amount),
        new HashEntry("customer_id", alert.CustomerId),
        new HashEntry("retry_count", 0),
        new HashEntry("current_step", "starting"),
        new HashEntry("context_data", JsonConvert.SerializeObject(
            new SessionContext
            {
                AlertSnapshot = alert,
                ProcessingHistory = new List<string>()
            }))
    };

    await db.HashSetAsync(sessionKey, sessionData);

    // Set TTL to prevent memory leaks
    await db.KeyExpireAsync(sessionKey, _sessionTimeout);

    // Add to active sessions set for monitoring
    await db.SetAddAsync("sessions:active", sessionId);
}

private async Task UpdateSessionStateAsync(
    string sessionId,
    string step,
    string state,
    Dictionary<string, object> additionalData = null)
{
    var db = _redis.GetDatabase();
    var sessionKey = $"session:{sessionId}";

    // Atomic update
    var transaction = db.CreateTransaction();

    transaction.HashSetAsync(sessionKey, "current_step", step);
    transaction.HashSetAsync(sessionKey, "state", state);
    transaction.HashSetAsync(sessionKey, "last_updated",
        DateTimeOffset.UtcNow.ToUnixTimeSeconds());

    if (additionalData != null)
    {
        foreach (var kvp in additionalData)
        {
            transaction.HashSetAsync(sessionKey, kvp.Key,
                JsonConvert.SerializeObject(kvp.Value));
        }
    }

    await transaction.ExecuteAsync();
}
```

---

## Semantic Kernel Implementation

### Kernel Configuration & Setup

```csharp
public class SemanticKernelBootstrapper
{
    public static IKernel BuildKernel(IConfiguration config)
    {
        // Create kernel builder
        var builder = Kernel.CreateBuilder();

        // Add Azure OpenAI service
        builder.Services.AddAzureOpenAIChatCompletion(
            deploymentName: config["AzureOpenAI:DeploymentName"],
            endpoint: config["AzureOpenAI:Endpoint"],
            apiKey: config["AzureOpenAI:ApiKey"],
            modelId: "gpt-4" // or gpt-4-turbo
        );

        // Add embeddings service
        builder.Services.AddAzureOpenAITextEmbeddingGeneration(
            deploymentName: config["AzureOpenAI:EmbeddingDeployment"],
            endpoint: config["AzureOpenAI:Endpoint"],
            apiKey: config["AzureOpenAI:ApiKey"],
            modelId: "text-embedding-ada-002"
        );

        // Add memory store (Redis-backed)
        builder.Services.AddSingleton<IMemoryStore>(sp =>
        {
            var redis = sp.GetRequiredService<IConnectionMultiplexer>();
            return new RedisMemoryStore(redis);
        });

        var kernel = builder.Build();

        // Register custom plugins
        RegisterFCMPlugins(kernel);

        return kernel;
    }

    private static void RegisterFCMPlugins(IKernel kernel)
    {
        // Alert Analysis Plugin
        kernel.ImportPluginFromType<AlertAnalysisPlugin>("AlertAnalysis");

        // Disposition Logic Plugin
        kernel.ImportPluginFromType<DispositionPlugin>("Disposition");

        // Rule Evaluation Plugin
        kernel.ImportPluginFromType<RuleEvaluationPlugin>("Rules");

        // Watchlist Plugin
        kernel.ImportPluginFromType<WatchlistPlugin>("Watchlist");

        // Similarity Search Plugin
        kernel.ImportPluginFromType<SimilaritySearchPlugin>("Similarity");
    }
}
```

### Custom Plugin Implementation Example

```csharp
public class AlertAnalysisPlugin
{
    private readonly IRepositoryFactory _repositoryFactory;
    private readonly ILogger _logger;

    public AlertAnalysisPlugin(IRepositoryFactory factory)
    {
        _repositoryFactory = factory;
        _logger = Logger.Get(typeof(AlertAnalysisPlugin));
    }

    [KernelFunction, Description("Analyzes an alert and extracts key features")]
    public async Task<AlertFeatures> AnalyzeAlertAsync(
        [Description("Alert ID to analyze")] int alertId,
        KernelArguments context)
    {
        var alertRepo = _repositoryFactory.GetAlertRepository();
        var alert = await alertRepo.GetByIdAsync(alertId);

        if (alert == null)
            throw new InvalidOperationException($"Alert {alertId} not found");

        // Extract features for ML/AI processing
        var features = new AlertFeatures
        {
            AlertId = alert.Id,
            Type = alert.Type,
            Amount = alert.Amount,
            AmountBucket = GetAmountBucket(alert.Amount),

            // Customer features
            CustomerRiskScore = await GetCustomerRiskScoreAsync(alert.CustomerId),
            CustomerAge = await GetCustomerAgeAsync(alert.CustomerId),
            CustomerAccountCount = await GetCustomerAccountCountAsync(alert.CustomerId),

            // Transaction features
            TransactionFrequency = await GetTransactionFrequencyAsync(
                alert.CustomerId, TimeSpan.FromDays(30)),
            AverageTransactionAmount = await GetAverageTransactionAmountAsync(
                alert.CustomerId),
            IsOffHours = IsOffHours(alert.TransactionDateTime),
            IsWeekend = alert.TransactionDateTime.DayOfWeek == DayOfWeek.Saturday ||
                       alert.TransactionDateTime.DayOfWeek == DayOfWeek.Sunday,

            // Account features
            AccountAge = await GetAccountAgeAsync(alert.AccountId),
            AccountType = alert.AccountType,
            AccountBalance = await GetAccountBalanceAsync(alert.AccountId),

            // Historical features
            PreviousAlertsCount = await GetPreviousAlertsCountAsync(
                alert.CustomerId, TimeSpan.FromDays(90)),
            PreviousFraudsCount = await GetPreviousFraudsCountAsync(
                alert.CustomerId),

            // Alert-specific
            AlertScore = alert.Score,
            AlertPriority = alert.Priority,
            Scenario = alert.Scenario,

            // Behavioral features
            DeviationFromNorm = await CalculateDeviationFromNormAsync(alert),
            VelocityScore = await CalculateVelocityScoreAsync(alert)
        };

        _logger.Info($"Extracted features for alert {alertId}",
            new Dictionary<string, object> { {"features", features} });

        return features;
    }

    [KernelFunction, Description("Gets historical context for an alert")]
    public async Task<AlertContext> GetAlertContextAsync(
        [Description("Alert ID")] int alertId,
        [Description("Number of days to look back")] int lookbackDays = 90)
    {
        var alertRepo = _repositoryFactory.GetAlertRepository();
        var alert = await alertRepo.GetByIdAsync(alertId);

        // Get related alerts
        var relatedAlerts = await alertRepo.GetRelatedAlertsAsync(
            alert.CustomerId,
            TimeSpan.FromDays(lookbackDays));

        // Get transaction history
        var transactionRepo = _repositoryFactory.GetTransactionRepository();
        var recentTransactions = await transactionRepo.GetCustomerTransactionsAsync(
            alert.CustomerId,
            DateTimeOffset.UtcNow.AddDays(-lookbackDays),
            DateTimeOffset.UtcNow);

        var context = new AlertContext
        {
            Alert = alert,
            RelatedAlerts = relatedAlerts.ToList(),
            RecentTransactions = recentTransactions.ToList(),
            CustomerProfile = await GetCustomerProfileAsync(alert.CustomerId),
            AccountProfile = await GetAccountProfileAsync(alert.AccountId)
        };

        return context;
    }

    private string GetAmountBucket(decimal amount)
    {
        if (amount < 100) return "micro";
        if (amount < 1000) return "small";
        if (amount < 10000) return "medium";
        if (amount < 50000) return "large";
        return "xlarge";
    }

    private bool IsOffHours(DateTime dt)
    {
        var hour = dt.Hour;
        return hour < 6 || hour > 22; // Before 6 AM or after 10 PM
    }
}
```

### Planner Implementation

```csharp
public class AlertTriagePlanner
{
    private readonly IKernel _kernel;

    public async Task<Plan> CreateTriagePlanAsync(Alert alert)
    {
        // Use Action Planner for single-step or Stepwise Planner for complex
        var planner = new HandlebarsPlanner();

        var goal = $@"
You are an expert fraud analyst. Analyze alert {alert.Id} and recommend a disposition.

ALERT DETAILS:
- Type: {alert.Type}
- Amount: ${alert.Amount:N2}
- Customer: {alert.CustomerId}
- Date: {alert.TransactionDateTime}
- Scenario: {alert.Scenario}

STEPS TO FOLLOW:
1. Use AlertAnalysis.AnalyzeAlertAsync to extract alert features
2. Use Similarity.FindSimilarAlertsAsync to find similar historical cases
3. Use Rules.EvaluateRulesAsync to check business rules
4. Use Watchlist.CheckWatchlistAsync to check if customer is on watchlist
5. Use Disposition.RecommendDispositionAsync to generate final recommendation

REQUIREMENTS:
- Provide disposition (Close, Escalate, Investigate)
- Include confidence score (0-1)
- Provide detailed reasoning
- Reference similar cases if found
- Note any rule violations
- Consider watchlist status

Return JSON format:
{{
    ""disposition"": ""Close|Escalate|Investigate"",
    ""confidence"": 0.0-1.0,
    ""reasoning"": ""detailed explanation"",
    ""similar_cases"": [list of similar alert IDs],
    ""rules_triggered"": [list of rule names],
    ""watchlist_hit"": true/false
}}
";

        var plan = await planner.CreatePlanAsync(_kernel, goal);
        return plan;
    }
}
```

### Prompt Templates

```csharp
public class PromptTemplateLibrary
{
    public const string AlertTriagePrompt = @"
You are an AI assistant helping fraud analysts make disposition decisions on alerts.

# Alert Information
Alert ID: {{alert_id}}
Type: {{alert_type}}
Amount: {{amount}}
Customer Risk Score: {{customer_risk_score}}
Transaction Time: {{transaction_time}}

# Similar Historical Cases
{{#each similar_cases}}
- Alert {{this.alert_id}}: Disposition={{this.disposition}}, Confidence={{this.confidence}}
  Reasoning: {{this.reasoning}}
{{/each}}

# Business Rules Evaluation
{{#each rules_triggered}}
- Rule: {{this.name}} - {{this.description}}
  Severity: {{this.severity}}
{{/each}}

# Analysis
Based on the above information, recommend a disposition and provide reasoning.

Consider:
1. Pattern matching with similar cases
2. Rule violations or triggers
3. Customer behavior patterns
4. Transaction characteristics
5. Risk indicators

# Output Format
Provide your response in JSON:
{
    ""disposition"": ""Close|Escalate|Investigate"",
    ""confidence"": 0.95,
    ""reasoning"": ""Detailed explanation"",
    ""key_factors"": [""factor1"", ""factor2""],
    ""recommended_actions"": [""action1"", ""action2""]
}
";

    public const string SimilaritySummaryPrompt = @"
# Similar Cases Summary
{{#each similar_cases}}
Case {{@index}}:
- Alert: {{this.alert_id}}
- Disposition: {{this.disposition}}
- Similarity Score: {{this.similarity_score}}
- Key Features: {{this.features}}
{{/each}}

Summarize the patterns and provide insights on how these cases should inform the current decision.
";
}
```

---

## Redis Storage Patterns - Deep Dive

### 1. Session Store - Advanced Implementation

```csharp
public class RedisSessionStore
{
    private readonly IDatabase _db;
    private readonly TimeSpan _sessionTTL = TimeSpan.FromHours(1);

    public async Task<string> CreateSessionAsync(Alert alert)
    {
        var sessionId = $"ses_{Guid.NewGuid():N}";
        var key = $"session:{sessionId}";

        // Use Redis Transaction for atomicity
        var transaction = _db.CreateTransaction();

        // Session metadata
        transaction.HashSetAsync(key, new HashEntry[]
        {
            new("alert_id", alert.Id),
            new("state", SessionState.Initialized.ToString()),
            new("started_at", DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            new("last_updated", DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            new("processing_stage", "initialization"),
            new("retry_count", 0),
            new("error_count", 0)
        });

        // Session data (larger, can be compressed)
        var contextData = new SessionContext
        {
            Alert = alert,
            ProcessingHistory = new List<ProcessingStep>(),
            IntermediateResults = new Dictionary<string, object>(),
            Metadata = new Dictionary<string, string>()
        };

        var serialized = JsonConvert.SerializeObject(contextData);
        var compressed = await CompressAsync(serialized);
        transaction.HashSetAsync(key, "context_data", compressed);

        // TTL
        transaction.KeyExpireAsync(key, _sessionTTL);

        // Add to active sessions index
        transaction.SetAddAsync("sessions:active", sessionId);
        transaction.SortedSetAddAsync("sessions:by_time", sessionId,
            DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        await transaction.ExecuteAsync();

        return sessionId;
    }

    public async Task UpdateProcessingStageAsync(
        string sessionId,
        string stage,
        Dictionary<string, object> stageResults = null)
    {
        var key = $"session:{sessionId}";

        // Update stage atomically
        var transaction = _db.CreateTransaction();

        transaction.HashSetAsync(key, "processing_stage", stage);
        transaction.HashSetAsync(key, "last_updated",
            DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        // If stage has results, store them
        if (stageResults != null)
        {
            var stageKey = $"{key}:stage:{stage}";
            var entries = stageResults.Select(kvp =>
                new HashEntry(kvp.Key, JsonConvert.SerializeObject(kvp.Value))
            ).ToArray();

            transaction.HashSetAsync(stageKey, entries);
            transaction.KeyExpireAsync(stageKey, _sessionTTL);

            // Add to processing history
            transaction.ListRightPushAsync($"{key}:history",
                $"{DateTimeOffset.UtcNow:O}|{stage}");
        }

        await transaction.ExecuteAsync();
    }

    public async Task<SessionContext> GetSessionContextAsync(string sessionId)
    {
        var key = $"session:{sessionId}";

        var compressedData = await _db.HashGetAsync(key, "context_data");
        if (compressedData.IsNullOrEmpty)
            return null;

        var decompressed = await DecompressAsync(compressedData);
        return JsonConvert.DeserializeObject<SessionContext>(decompressed);
    }

    // Cleanup expired or completed sessions
    public async Task CleanupSessionsAsync()
    {
        // Find sessions older than TTL
        var cutoffTime = DateTimeOffset.UtcNow.AddHours(-2).ToUnixTimeSeconds();
        var expiredSessions = await _db.SortedSetRangeByScoreAsync(
            "sessions:by_time",
            0,
            cutoffTime);

        foreach (var sessionId in expiredSessions)
        {
            await DeleteSessionAsync(sessionId.ToString());
        }
    }
}
```

### 2. Decision History - Vector-Enabled Storage

```csharp
public class RedisDecisionHistoryStore
{
    private readonly IDatabase _db;
    private readonly ITextEmbeddingGenerationService _embeddingService;

    public async Task StoreDecisionAsync(
        Alert alert,
        DispositionRecommendation recommendation,
        AlertFeatures features)
    {
        var alertHash = ComputeAlertHash(alert);
        var decisionId = $"dec_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}_{alert.Id}";

        // Store decision metadata
        var decisionKey = $"decision:{alertHash}";
        await _db.HashSetAsync(decisionKey, new HashEntry[]
        {
            new("decision_id", decisionId),
            new("alert_id", alert.Id),
            new("disposition", recommendation.Disposition),
            new("confidence", recommendation.Confidence),
            new("reasoning", recommendation.Reasoning),
            new("analyst_id", recommendation.AnalystId ?? "AI"),
            new("is_ai_decision", recommendation.IsAIGenerated),
            new("timestamp", DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            new("alert_type", alert.Type),
            new("amount", (double)alert.Amount),
            new("customer_id", alert.CustomerId),
            new("features", JsonConvert.SerializeObject(features))
        });

        // Set retention period (e.g., 180 days)
        await _db.KeyExpireAsync(decisionKey, TimeSpan.FromDays(180));

        // Add to chronological index
        await _db.SortedSetAddAsync(
            "decisions:chronological",
            alertHash,
            DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        // Add to disposition-specific index
        await _db.SetAddAsync(
            $"decisions:by_disposition:{recommendation.Disposition}",
            alertHash);

        // Add to customer index
        await _db.SetAddAsync(
            $"decisions:by_customer:{alert.CustomerId}",
            alertHash);

        // Generate and store embedding for semantic search
        var embeddingText = CreateEmbeddingText(alert, features, recommendation);
        var embedding = await _embeddingService.GenerateEmbeddingAsync(embeddingText);

        // Store embedding (Redis Vector Search)
        await StoreEmbeddingAsync(alertHash, embedding, alert, recommendation);

        // Update aggregated statistics
        await UpdateDecisionStatsAsync(recommendation.Disposition, recommendation.IsAIGenerated);
    }

    private async Task StoreEmbeddingAsync(
        string alertHash,
        ReadOnlyMemory<float> embedding,
        Alert alert,
        DispositionRecommendation recommendation)
    {
        // Redis Vector Search using FT.CREATE and FT.ADD
        // This requires Redis Stack with RediSearch module

        var vectorKey = $"vector:{alertHash}";

        await _db.HashSetAsync(vectorKey, new HashEntry[]
        {
            new("alert_id", alert.Id),
            new("alert_hash", alertHash),
            new("disposition", recommendation.Disposition),
            new("confidence", recommendation.Confidence),
            new("timestamp", DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            new("alert_type", alert.Type),
            new("amount", (double)alert.Amount),
            new("embedding", SerializeVector(embedding))
        });

        await _db.KeyExpireAsync(vectorKey, TimeSpan.FromDays(180));
    }

    private string CreateEmbeddingText(
        Alert alert,
        AlertFeatures features,
        DispositionRecommendation recommendation)
    {
        // Create rich text representation for embedding
        return $@"
Alert Type: {alert.Type}
Scenario: {alert.Scenario}
Amount: {alert.Amount} ({features.AmountBucket})
Customer Risk: {features.CustomerRiskScore}
Account Age: {features.AccountAge} days
Transaction Time: {(features.IsOffHours ? "Off-hours" : "Business hours")} {(features.IsWeekend ? "Weekend" : "Weekday")}
Previous Alerts: {features.PreviousAlertsCount}
Disposition: {recommendation.Disposition}
Reasoning: {recommendation.Reasoning}
Key Factors: {string.Join(", ", recommendation.KeyFactors)}
";
    }

    private string ComputeAlertHash(Alert alert)
    {
        // Create a hash based on key alert attributes
        var data = $"{alert.Type}|{alert.Amount}|{alert.CustomerId}|{alert.TransactionDateTime:yyyyMMdd}|{alert.Scenario}";
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(Encoding.UTF8.GetBytes(data));
        return Convert.ToBase64String(hash).Substring(0, 16);
    }
}
```

### 3. Vector Search Implementation

```csharp
public class RedisSimilaritySearcher
{
    private readonly IConnectionMultiplexer _redis;
    private readonly ITextEmbeddingGenerationService _embeddingService;
    private const string VectorIndexName = "alert_vectors_idx";

    public async Task InitializeVectorIndexAsync()
    {
        // Create Redis Vector Search index
        // FT.CREATE alert_vectors_idx
        //   ON HASH PREFIX 1 vector:
        //   SCHEMA
        //     alert_id NUMERIC SORTABLE
        //     disposition TAG
        //     amount NUMERIC SORTABLE
        //     timestamp NUMERIC SORTABLE
        //     embedding VECTOR HNSW 6
        //       TYPE FLOAT32
        //       DIM 1536
        //       DISTANCE_METRIC COSINE

        var db = _redis.GetDatabase();

        try
        {
            await db.ExecuteAsync("FT.CREATE",
                VectorIndexName,
                "ON", "HASH",
                "PREFIX", "1", "vector:",
                "SCHEMA",
                "alert_id", "NUMERIC", "SORTABLE",
                "disposition", "TAG",
                "alert_type", "TAG",
                "amount", "NUMERIC", "SORTABLE",
                "confidence", "NUMERIC", "SORTABLE",
                "timestamp", "NUMERIC", "SORTABLE",
                "embedding", "VECTOR", "HNSW", "6",
                    "TYPE", "FLOAT32",
                    "DIM", "1536",
                    "DISTANCE_METRIC", "COSINE");
        }
        catch (RedisServerException ex) when (ex.Message.Contains("Index already exists"))
        {
            // Index already exists, ignore
        }
    }

    public async Task<List<SimilarAlert>> FindSimilarAlertsAsync(
        Alert alert,
        AlertFeatures features,
        int topK = 10,
        double minSimilarity = 0.7)
    {
        // Generate embedding for current alert
        var embeddingText = CreateEmbeddingText(alert, features);
        var embedding = await _embeddingService.GenerateEmbeddingAsync(embeddingText);

        // Perform KNN vector search
        var db = _redis.GetDatabase();

        // FT.SEARCH alert_vectors_idx
        //   "*=>[KNN 10 @embedding $vec AS similarity]"
        //   PARAMS 2 vec <embedding_blob>
        //   SORTBY similarity
        //   RETURN 5 alert_id disposition confidence timestamp similarity
        //   DIALECT 2

        var vectorBlob = SerializeVector(embedding);

        var result = await db.ExecuteAsync("FT.SEARCH",
            VectorIndexName,
            $"*=>[KNN {topK} @embedding $vec AS similarity]",
            "PARAMS", "2", "vec", vectorBlob,
            "SORTBY", "similarity",
            "RETURN", "6",
                "alert_id", "disposition", "confidence", "timestamp", "amount", "similarity",
            "DIALECT", "2");

        return ParseSearchResults(result, minSimilarity);
    }

    public async Task<List<SimilarAlert>> FindSimilarAlertsWithFiltersAsync(
        Alert alert,
        AlertFeatures features,
        string dispositionFilter = null,
        decimal? minAmount = null,
        decimal? maxAmount = null,
        int topK = 10)
    {
        var embeddingText = CreateEmbeddingText(alert, features);
        var embedding = await _embeddingService.GenerateEmbeddingAsync(embeddingText);
        var vectorBlob = SerializeVector(embedding);

        // Build filter query
        var filters = new List<string>();

        if (!string.IsNullOrEmpty(dispositionFilter))
            filters.Add($"@disposition:{{{dispositionFilter}}}");

        if (minAmount.HasValue)
            filters.Add($"@amount:[{minAmount} +inf]");

        if (maxAmount.HasValue)
            filters.Add($"@amount:[-inf {maxAmount}]");

        var filterQuery = filters.Any()
            ? $"({string.Join(" ", filters)})"
            : "*";

        var db = _redis.GetDatabase();

        var result = await db.ExecuteAsync("FT.SEARCH",
            VectorIndexName,
            $"{filterQuery}=>[KNN {topK} @embedding $vec AS similarity]",
            "PARAMS", "2", "vec", vectorBlob,
            "SORTBY", "similarity",
            "RETURN", "7",
                "alert_id", "disposition", "confidence", "timestamp",
                "amount", "alert_type", "similarity",
            "DIALECT", "2");

        return ParseSearchResults(result, 0.0);
    }

    private List<SimilarAlert> ParseSearchResults(
        RedisResult result,
        double minSimilarity)
    {
        var results = new List<SimilarAlert>();

        // Redis returns: [total_count, [key, [field, value, field, value, ...]], ...]
        var array = (RedisResult[])result;

        if (array.Length < 2)
            return results;

        var totalCount = (int)array[0];

        for (int i = 1; i < array.Length; i += 2)
        {
            var key = (string)array[i];
            var fields = (RedisResult[])array[i + 1];

            var similarAlert = new SimilarAlert
            {
                AlertHash = key.Replace("vector:", "")
            };

            for (int j = 0; j < fields.Length; j += 2)
            {
                var fieldName = (string)fields[j];
                var fieldValue = fields[j + 1];

                switch (fieldName)
                {
                    case "alert_id":
                        similarAlert.AlertId = (int)fieldValue;
                        break;
                    case "disposition":
                        similarAlert.Disposition = (string)fieldValue;
                        break;
                    case "confidence":
                        similarAlert.Confidence = (double)fieldValue;
                        break;
                    case "similarity":
                        similarAlert.SimilarityScore = 1.0 - (double)fieldValue; // Convert distance to similarity
                        break;
                    case "timestamp":
                        similarAlert.Timestamp = DateTimeOffset.FromUnixTimeSeconds((long)fieldValue);
                        break;
                    case "amount":
                        similarAlert.Amount = (decimal)(double)fieldValue;
                        break;
                    case "alert_type":
                        similarAlert.AlertType = (string)fieldValue;
                        break;
                }
            }

            if (similarAlert.SimilarityScore >= minSimilarity)
                results.Add(similarAlert);
        }

        return results.OrderByDescending(x => x.SimilarityScore).ToList();
    }

    private byte[] SerializeVector(ReadOnlyMemory<float> embedding)
    {
        var span = embedding.Span;
        var bytes = new byte[span.Length * sizeof(float)];
        Buffer.BlockCopy(span.ToArray(), 0, bytes, 0, bytes.Length);
        return bytes;
    }
}
```

### 4. Criteria Cache - Smart Caching

```csharp
public class RedisCriteriaCache
{
    private readonly IDatabase _db;
    private readonly IRepositoryFactory _repositoryFactory;
    private readonly TimeSpan _cacheExpiry = TimeSpan.FromMinutes(5);

    public async Task<List<BusinessRule>> GetActiveRulesAsync(
        bool forceRefresh = false)
    {
        var cacheKey = "rules:active_list";

        if (!forceRefresh)
        {
            // Try to get from cache
            var cachedData = await _db.StringGetAsync(cacheKey);
            if (!cachedData.IsNullOrEmpty)
            {
                return JsonConvert.DeserializeObject<List<BusinessRule>>(cachedData);
            }
        }

        // Cache miss or force refresh - get from database
        var ruleRepo = _repositoryFactory.GetBusinessRuleRepository();
        var rules = await ruleRepo.GetActiveRulesAsync();

        // Store in cache
        var serialized = JsonConvert.SerializeObject(rules);
        await _db.StringSetAsync(cacheKey, serialized, _cacheExpiry);

        // Also store individual rules for quick lookup
        foreach (var rule in rules)
        {
            await CacheRuleAsync(rule);
        }

        return rules;
    }

    private async Task CacheRuleAsync(BusinessRule rule)
    {
        var ruleKey = $"rule:{rule.Id}";

        await _db.HashSetAsync(ruleKey, new HashEntry[]
        {
            new("id", rule.Id),
            new("name", rule.Name),
            new("description", rule.Description),
            new("condition_expression", rule.ConditionExpression),
            new("action", rule.Action),
            new("priority", rule.Priority),
            new("severity", rule.Severity),
            new("enabled", rule.Enabled),
            new("category", rule.Category),
            new("updated_at", rule.UpdatedAt.ToUnixTimeSeconds())
        });

        await _db.KeyExpireAsync(ruleKey, _cacheExpiry);

        // Add to category index
        await _db.SetAddAsync($"rules:category:{rule.Category}", rule.Id);
    }

    public async Task InvalidateRuleCacheAsync(int ruleId)
    {
        // Remove specific rule
        await _db.KeyDeleteAsync($"rule:{ruleId}");

        // Remove from active list
        await _db.KeyDeleteAsync("rules:active_list");

        // Publish invalidation message for distributed cache
        await _db.PublishAsync("cache:invalidation", $"rule:{ruleId}");
    }

    public async Task<Dictionary<string, ThresholdConfig>> GetThresholdsAsync()
    {
        var cacheKey = "config:thresholds";
        var cached = await _db.StringGetAsync(cacheKey);

        if (!cached.IsNullOrEmpty)
        {
            return JsonConvert.DeserializeObject<Dictionary<string, ThresholdConfig>>(cached);
        }

        // Load from database
        var configRepo = _repositoryFactory.GetConfigurationRepository();
        var thresholds = await configRepo.GetThresholdsAsync();

        // Cache for longer period (thresholds change infrequently)
        await _db.StringSetAsync(
            cacheKey,
            JsonConvert.SerializeObject(thresholds),
            TimeSpan.FromHours(1));

        return thresholds;
    }
}
```

### 5. Performance Metrics - Time Series

```csharp
public class RedisMetricsStore
{
    private readonly IDatabase _db;

    public async Task RecordDispositionMetricAsync(
        int alertId,
        string disposition,
        double confidence,
        long processingTimeMs,
        bool isAuto,
        bool wasOverridden = false)
    {
        var timestamp = DateTimeOffset.UtcNow;
        var date = timestamp.ToString("yyyy-MM-dd");

        // Add to metrics stream (time-ordered log)
        await _db.StreamAddAsync(
            "metrics:dispositions",
            new[]
            {
                new NameValueEntry("alert_id", alertId),
                new NameValueEntry("disposition", disposition),
                new NameValueEntry("confidence", confidence),
                new NameValueEntry("processing_time_ms", processingTimeMs),
                new NameValueEntry("is_auto", isAuto),
                new NameValueEntry("was_overridden", wasOverridden),
                new NameValueEntry("timestamp", timestamp.ToUnixTimeSeconds())
            });

        // Update daily counters
        var transaction = _db.CreateTransaction();

        transaction.StringIncrementAsync($"metrics:daily:{date}:total");

        if (isAuto)
            transaction.StringIncrementAsync($"metrics:daily:{date}:auto_applied");
        else
            transaction.StringIncrementAsync($"metrics:daily:{date}:analyst_reviewed");

        if (wasOverridden)
            transaction.StringIncrementAsync($"metrics:daily:{date}:overridden");

        transaction.StringIncrementAsync($"metrics:daily:{date}:disposition:{disposition}");

        // Update processing time stats (using sorted set for percentiles)
        transaction.SortedSetAddAsync(
            $"metrics:processing_times:{date}",
            alertId.ToString(),
            processingTimeMs);

        // Update confidence distribution
        var confidenceBucket = Math.Floor(confidence * 10) / 10; // 0.0, 0.1, 0.2, etc.
        transaction.StringIncrementAsync(
            $"metrics:confidence:{date}:{confidenceBucket:F1}");

        await transaction.ExecuteAsync();

        // Set expiry on daily keys (keep for 90 days)
        await _db.KeyExpireAsync($"metrics:daily:{date}:total", TimeSpan.FromDays(90));
    }

    public async Task<DailyMetrics> GetDailyMetricsAsync(DateTime date)
    {
        var dateStr = date.ToString("yyyy-MM-dd");

        var metrics = new DailyMetrics
        {
            Date = date,
            TotalProcessed = (long)await _db.StringGetAsync($"metrics:daily:{dateStr}:total"),
            AutoApplied = (long)await _db.StringGetAsync($"metrics:daily:{dateStr}:auto_applied"),
            AnalystReviewed = (long)await _db.StringGetAsync($"metrics:daily:{dateStr}:analyst_reviewed"),
            Overridden = (long)await _db.StringGetAsync($"metrics:daily:{dateStr}:overridden")
        };

        // Get disposition breakdown
        metrics.DispositionBreakdown = new Dictionary<string, long>();
        var dispositions = new[] { "Close", "Escalate", "Investigate" };
        foreach (var disposition in dispositions)
        {
            var count = (long)await _db.StringGetAsync(
                $"metrics:daily:{dateStr}:disposition:{disposition}");
            metrics.DispositionBreakdown[disposition] = count;
        }

        // Calculate processing time percentiles
        var processingTimes = await _db.SortedSetRangeByRankAsync(
            $"metrics:processing_times:{dateStr}",
            0, -1,
            Order.Ascending);

        if (processingTimes.Length > 0)
        {
            var times = processingTimes.Select(x => (long)x.Score).ToList();
            metrics.ProcessingTimeP50 = GetPercentile(times, 0.50);
            metrics.ProcessingTimeP95 = GetPercentile(times, 0.95);
            metrics.ProcessingTimeP99 = GetPercentile(times, 0.99);
        }

        return metrics;
    }

    private long GetPercentile(List<long> sortedValues, double percentile)
    {
        if (sortedValues.Count == 0) return 0;
        var index = (int)Math.Ceiling(sortedValues.Count * percentile) - 1;
        return sortedValues[Math.Max(0, Math.Min(index, sortedValues.Count - 1))];
    }

    // Real-time monitoring
    public async Task<RealtimeMetrics> GetRealtimeMetricsAsync()
    {
        var activeSessions = await _db.SetLengthAsync("sessions:active");
        var queueDepth = await _db.ListLengthAsync("alerts:pending_queue");

        // Get processing rate (last 5 minutes from stream)
        var fiveMinutesAgo = DateTimeOffset.UtcNow.AddMinutes(-5).ToUnixTimeMilliseconds();
        var recentEntries = await _db.StreamReadAsync(
            "metrics:dispositions",
            $"{fiveMinutesAgo}-0",
            count: 1000);

        return new RealtimeMetrics
        {
            ActiveSessions = activeSessions,
            QueueDepth = queueDepth,
            ProcessingRatePer5Min = recentEntries.Length,
            Timestamp = DateTimeOffset.UtcNow
        };
    }
}
```

---

## Decision Engine Algorithms

### Similarity Scoring Algorithm

```csharp
public class SimilarityScorer
{
    public async Task<SimilarityScore> CalculateSimilarityAsync(
        Alert currentAlert,
        AlertFeatures currentFeatures,
        Alert historicalAlert,
        AlertFeatures historicalFeatures)
    {
        var score = new SimilarityScore();

        // 1. Semantic similarity (from vector embeddings) - Weight: 40%
        score.SemanticSimilarity = currentFeatures.SemanticSimilarity; // From vector search

        // 2. Feature-based similarity - Weight: 30%
        score.FeatureSimilarity = CalculateFeatureSimilarity(
            currentFeatures,
            historicalFeatures);

        // 3. Behavioral similarity - Weight: 20%
        score.BehavioralSimilarity = CalculateBehavioralSimilarity(
            currentFeatures,
            historicalFeatures);

        // 4. Contextual similarity - Weight: 10%
        score.ContextualSimilarity = CalculateContextualSimilarity(
            currentAlert,
            historicalAlert);

        // Weighted combination
        score.OverallSimilarity =
            (score.SemanticSimilarity * 0.40) +
            (score.FeatureSimilarity * 0.30) +
            (score.BehavioralSimilarity * 0.20) +
            (score.ContextualSimilarity * 0.10);

        return score;
    }

    private double CalculateFeatureSimilarity(
        AlertFeatures current,
        AlertFeatures historical)
    {
        var similarities = new List<double>();

        // Amount similarity (using log scale)
        var amountSim = 1.0 - Math.Min(1.0,
            Math.Abs(Math.Log10(current.Amount) - Math.Log10(historical.Amount)) / 4.0);
        similarities.Add(amountSim);

        // Customer risk score similarity
        var riskSim = 1.0 - Math.Abs(current.CustomerRiskScore - historical.CustomerRiskScore) / 100.0;
        similarities.Add(riskSim);

        // Account age similarity (bucketed)
        var accountAgeSim = current.AccountAge == historical.AccountAge ? 1.0 :
            Math.Max(0.0, 1.0 - Math.Abs(current.AccountAge - historical.AccountAge) / 365.0);
        similarities.Add(accountAgeSim);

        // Categorical features (exact match)
        similarities.Add(current.AlertType == historical.AlertType ? 1.0 : 0.0);
        similarities.Add(current.IsOffHours == historical.IsOffHours ? 1.0 : 0.0);
        similarities.Add(current.IsWeekend == historical.IsWeekend ? 1.0 : 0.0);

        return similarities.Average();
    }

    private double CalculateBehavioralSimilarity(
        AlertFeatures current,
        AlertFeatures historical)
    {
        var similarities = new List<double>();

        // Transaction frequency
        var freqDiff = Math.Abs(current.TransactionFrequency - historical.TransactionFrequency);
        similarities.Add(Math.Max(0.0, 1.0 - freqDiff / 100.0));

        // Velocity score
        var velocitySim = 1.0 - Math.Abs(current.VelocityScore - historical.VelocityScore);
        similarities.Add(Math.Max(0.0, velocitySim));

        // Previous alerts pattern
        var alertCountDiff = Math.Abs(current.PreviousAlertsCount - historical.PreviousAlertsCount);
        similarities.Add(Math.Max(0.0, 1.0 - alertCountDiff / 10.0));

        return similarities.Average();
    }

    private double CalculateContextualSimilarity(
        Alert current,
        Alert historical)
    {
        var score = 0.0;

        // Same scenario
        if (current.Scenario == historical.Scenario)
            score += 0.4;

        // Similar time period (same day of week, similar hour)
        if (current.TransactionDateTime.DayOfWeek == historical.TransactionDateTime.DayOfWeek)
            score += 0.2;

        var hourDiff = Math.Abs(current.TransactionDateTime.Hour - historical.TransactionDateTime.Hour);
        if (hourDiff <= 2)
            score += 0.2;

        // Seasonal similarity (same quarter)
        if (GetQuarter(current.TransactionDateTime) == GetQuarter(historical.TransactionDateTime))
            score += 0.2;

        return score;
    }

    private int GetQuarter(DateTime date)
    {
        return (date.Month - 1) / 3 + 1;
    }
}
```

### Disposition Recommendation Algorithm

```csharp
public class DispositionRecommender
{
    private readonly ILogger _logger;

    public DispositionRecommendation RecommendDisposition(
        Alert alert,
        AlertFeatures features,
        List<SimilarAlert> similarCases,
        List<RuleEvaluation> ruleResults,
        string llmReasoning)
    {
        var recommendation = new DispositionRecommendation
        {
            AlertId = alert.Id,
            Timestamp = DateTimeOffset.UtcNow
        };

        // Calculate component scores
        var similarityScore = CalculateSimilarityBasedScore(similarCases);
        var ruleScore = CalculateRuleBasedScore(ruleResults);
        var riskScore = CalculateRiskScore(features);

        // Weighted combination
        var closeScore = similarityScore.CloseScore * 0.4 +
                        ruleScore.CloseScore * 0.3 +
                        riskScore.CloseScore * 0.3;

        var escalateScore = similarityScore.EscalateScore * 0.4 +
                           ruleScore.EscalateScore * 0.3 +
                           riskScore.EscalateScore * 0.3;

        var investigateScore = similarityScore.InvestigateScore * 0.4 +
                              ruleScore.InvestigateScore * 0.3 +
                              riskScore.InvestigateScore * 0.3;

        // Select disposition with highest score
        var scores = new Dictionary<string, double>
        {
            { "Close", closeScore },
            { "Escalate", escalateScore },
            { "Investigate", investigateScore }
        };

        var maxScore = scores.Max(x => x.Value);
        recommendation.Disposition = scores.First(x => x.Value == maxScore).Key;
        recommendation.Confidence = maxScore;

        // Build reasoning
        recommendation.Reasoning = BuildReasoning(
            recommendation.Disposition,
            similarCases,
            ruleResults,
            features,
            llmReasoning);

        // Identify key factors
        recommendation.KeyFactors = IdentifyKeyFactors(
            features,
            similarCases,
            ruleResults);

        // Recommended actions
        recommendation.RecommendedActions = GenerateRecommendedActions(
            recommendation.Disposition,
            features,
            ruleResults);

        _logger.Info($"Generated recommendation for alert {alert.Id}",
            new Dictionary<string, object>
            {
                {"disposition", recommendation.Disposition},
                {"confidence", recommendation.Confidence},
                {"similar_cases_count", similarCases.Count},
                {"rules_triggered", ruleResults.Count(r => r.Triggered)}
            });

        return recommendation;
    }

    private DispositionScores CalculateSimilarityBasedScore(
        List<SimilarAlert> similarCases)
    {
        var scores = new DispositionScores();

        if (!similarCases.Any())
        {
            // No history, default to investigate
            scores.InvestigateScore = 0.5;
            return scores;
        }

        // Weight by similarity score
        var totalWeight = similarCases.Sum(x => x.SimilarityScore);

        foreach (var similar in similarCases)
        {
            var weight = similar.SimilarityScore / totalWeight;

            switch (similar.Disposition)
            {
                case "Close":
                    scores.CloseScore += weight * similar.Confidence;
                    break;
                case "Escalate":
                    scores.EscalateScore += weight * similar.Confidence;
                    break;
                case "Investigate":
                    scores.InvestigateScore += weight * similar.Confidence;
                    break;
            }
        }

        return scores;
    }

    private DispositionScores CalculateRuleBasedScore(
        List<RuleEvaluation> ruleResults)
    {
        var scores = new DispositionScores();

        foreach (var rule in ruleResults.Where(r => r.Triggered))
        {
            switch (rule.Severity)
            {
                case "Critical":
                    scores.EscalateScore += 0.8;
                    break;
                case "High":
                    scores.InvestigateScore += 0.6;
                    break;
                case "Medium":
                    scores.InvestigateScore += 0.4;
                    break;
                case "Low":
                    scores.CloseScore += 0.3;
                    break;
            }
        }

        // Normalize
        var total = scores.CloseScore + scores.EscalateScore + scores.InvestigateScore;
        if (total > 0)
        {
            scores.CloseScore /= total;
            scores.EscalateScore /= total;
            scores.InvestigateScore /= total;
        }

        return scores;
    }

    private DispositionScores CalculateRiskScore(AlertFeatures features)
    {
        var scores = new DispositionScores();

        // High risk indicators
        if (features.CustomerRiskScore > 80)
            scores.EscalateScore += 0.5;
        else if (features.CustomerRiskScore > 60)
            scores.InvestigateScore += 0.4;
        else
            scores.CloseScore += 0.3;

        // Large amount
        if (features.Amount > 50000)
            scores.EscalateScore += 0.3;
        else if (features.Amount > 10000)
            scores.InvestigateScore += 0.2;

        // Previous fraud
        if (features.PreviousFraudsCount > 0)
            scores.EscalateScore += 0.7;

        // Off-hours + weekend
        if (features.IsOffHours && features.IsWeekend)
            scores.InvestigateScore += 0.3;

        // High velocity
        if (features.VelocityScore > 0.8)
            scores.EscalateScore += 0.4;

        // Normalize
        var total = scores.CloseScore + scores.EscalateScore + scores.InvestigateScore;
        if (total > 0)
        {
            scores.CloseScore /= total;
            scores.EscalateScore /= total;
            scores.InvestigateScore /= total;
        }

        return scores;
    }

    private string BuildReasoning(
        string disposition,
        List<SimilarAlert> similarCases,
        List<RuleEvaluation> ruleResults,
        AlertFeatures features,
        string llmReasoning)
    {
        var reasoning = new StringBuilder();

        reasoning.AppendLine($"Recommended Disposition: {disposition}");
        reasoning.AppendLine();

        reasoning.AppendLine("Key Analysis Points:");

        // Similar cases
        if (similarCases.Any())
        {
            reasoning.AppendLine($"- Found {similarCases.Count} similar historical cases:");
            foreach (var similar in similarCases.Take(3))
            {
                reasoning.AppendLine($"  • Alert {similar.AlertId}: {similar.Disposition} " +
                    $"(Similarity: {similar.SimilarityScore:P0}, Confidence: {similar.Confidence:P0})");
            }
        }
        else
        {
            reasoning.AppendLine("- No similar historical cases found (novel pattern)");
        }

        reasoning.AppendLine();

        // Rules triggered
        var triggeredRules = ruleResults.Where(r => r.Triggered).ToList();
        if (triggeredRules.Any())
        {
            reasoning.AppendLine($"- {triggeredRules.Count} business rules triggered:");
            foreach (var rule in triggeredRules)
            {
                reasoning.AppendLine($"  • {rule.RuleName} ({rule.Severity})");
            }
        }
        else
        {
            reasoning.AppendLine("- No business rules triggered");
        }

        reasoning.AppendLine();

        // Risk factors
        reasoning.AppendLine("- Risk Assessment:");
        reasoning.AppendLine($"  • Customer Risk Score: {features.CustomerRiskScore}");
        reasoning.AppendLine($"  • Amount: ${features.Amount:N2} ({features.AmountBucket})");
        reasoning.AppendLine($"  • Previous Alerts: {features.PreviousAlertsCount}");
        reasoning.AppendLine($"  • Previous Frauds: {features.PreviousFraudsCount}");

        if (!string.IsNullOrEmpty(llmReasoning))
        {
            reasoning.AppendLine();
            reasoning.AppendLine("AI Analysis:");
            reasoning.AppendLine(llmReasoning);
        }

        return reasoning.ToString();
    }
}
```

---

## Integration Architecture

### FCM Integration Points

#### 1. UnBusinessLayer Integration

```csharp
public class AlertTriageManager : IAlertTriageManager
{
    private readonly IAlertHistory _alertHistory;
    private readonly IWorkflowManager _workflowManager;
    private readonly IOrchestrationManager _orchestrationManager;
    private readonly AlertTriageOrchestrator _triageOrchestrator;

    public async Task<DispositionRecommendation> GetRecommendationAsync(int alertId)
    {
        // Called from UnWebApp API controller
        var alert = await _alertHistory.GetAlertByIdAsync(alertId);

        if (alert == null)
            throw new NotFoundException($"Alert {alertId} not found");

        // Request recommendation from triage agent
        var recommendation = await _triageOrchestrator.ProcessAlertAsync(alert);

        return recommendation;
    }

    public async Task ApplyDispositionAsync(
        int alertId,
        string disposition,
        string reasoning,
        string analystId)
    {
        // Update alert in database
        await _alertHistory.UpdateDispositionAsync(
            alertId,
            disposition,
            reasoning,
            analystId);

        // Update workflow state
        await _workflowManager.CompleteWorkItemAsync(alertId);

        // Store in Redis for learning
        await StoreAnalystDecisionAsync(alertId, disposition, reasoning, analystId);
    }

    public async Task RecordAnalystOverrideAsync(
        int alertId,
        string originalDisposition,
        string overrideDisposition,
        string reason,
        string analystId)
    {
        // Record when analyst overrides AI recommendation
        await _alertHistory.RecordOverrideAsync(
            alertId,
            originalDisposition,
            overrideDisposition,
            reason,
            analystId);

        // Feedback to learning system
        await _triageOrchestrator.RecordFeedbackAsync(
            alertId,
            overrideDisposition,
            reason,
            isCorrection: true);
    }
}
```

#### 2. UnDataAccessLayer Integration

```csharp
public interface IAlertTriageRepository : IRepository
{
    Task<List<Alert>> GetPendingAlertsAsync(int maxCount = 100);
    Task<AlertFeatures> GetAlertFeaturesAsync(int alertId);
    Task<List<SimilarAlert>> GetHistoricalSimilarAlertsAsync(
        int alertId,
        int lookbackDays = 90);
    Task StoreDispositionRecommendationAsync(
        int alertId,
        DispositionRecommendation recommendation);
    Task<List<AnalystOverride>> GetAnalystOverridesAsync(
        DateTime startDate,
        DateTime endDate);
}

public class AlertTriageRepository : IAlertTriageRepository
{
    private readonly IDbConnection _connection;
    private readonly ILogger _logger;

    public async Task<List<Alert>> GetPendingAlertsAsync(int maxCount = 100)
    {
        const string sql = @"
            SELECT TOP (@MaxCount)
                a.AlertId,
                a.AlertType,
                a.Amount,
                a.CustomerId,
                a.AccountId,
                a.TransactionDateTime,
                a.Scenario,
                a.Score,
                a.Priority,
                a.Status
            FROM Alerts a
            WHERE a.Status = 'Pending'
                AND a.AssignedTo IS NULL
            ORDER BY a.Priority DESC, a.CreatedDate ASC";

        var alerts = await _connection.QueryAsync<Alert>(
            sql,
            new { MaxCount = maxCount });

        return alerts.ToList();
    }

    public async Task StoreDispositionRecommendationAsync(
        int alertId,
        DispositionRecommendation recommendation)
    {
        const string sql = @"
            INSERT INTO AlertDispositionRecommendations
                (AlertId, Disposition, Confidence, Reasoning,
                 SimilarCasesUsed, RulesTriggered, RecommendedBy, CreatedDate)
            VALUES
                (@AlertId, @Disposition, @Confidence, @Reasoning,
                 @SimilarCasesUsed, @RulesTriggered, 'AI', GETUTCDATE())";

        await _connection.ExecuteAsync(sql, new
        {
            AlertId = alertId,
            Disposition = recommendation.Disposition,
            Confidence = recommendation.Confidence,
            Reasoning = recommendation.Reasoning,
            SimilarCasesUsed = JsonConvert.SerializeObject(recommendation.SimilarCases),
            RulesTriggered = JsonConvert.SerializeObject(recommendation.RulesTriggered)
        });
    }
}
```

#### 3. BankFraud Pipeline Integration

```csharp
public class AlertTriageStage : Stage
{
    private readonly IUnityContainer _container;
    private readonly IRepositoryFactory _factory;
    private readonly IKernel _semanticKernel;
    private readonly IConnectionMultiplexer _redis;

    public AlertTriageStage(IUnityContainer container)
    {
        _container = container;

        if (!container.IsRegistered<IRepositoryFactory>())
            throw new ApplicationException("Missing IRepositoryFactory registration");

        _factory = container.Resolve<IRepositoryFactory>();
        _semanticKernel = container.Resolve<IKernel>();
        _redis = container.Resolve<IConnectionMultiplexer>();
    }

    public override async Task<bool> DetailAsync(
        IContext context,
        IDictionary<string, IType> dictionary)
    {
        // Extract alert ID from context
        var alertIdStr = dictionary.ContainsKey("alert_id")
            ? dictionary["alert_id"].ToString()
            : null;

        if (string.IsNullOrEmpty(alertIdStr) || !int.TryParse(alertIdStr, out var alertId))
        {
            context.Logger.Error("Invalid or missing alert_id in context");
            return false;
        }

        try
        {
            // Get alert
            var alertRepo = _factory.GetAlertRepository();
            var alert = await alertRepo.GetByIdAsync(alertId);

            if (alert == null)
            {
                context.Logger.Error($"Alert {alertId} not found");
                return false;
            }

            // Get recommendation from triage agent
            var orchestrator = new AlertTriageOrchestrator(_container, _factory);
            var recommendation = await orchestrator.ProcessSingleAlertAsync(alert);

            // Store recommendation in context for next stage
            dictionary["triage_disposition"] =
                new StringType(recommendation.Disposition);
            dictionary["triage_confidence"] =
                new DecimalType((decimal)recommendation.Confidence);
            dictionary["triage_reasoning"] =
                new StringType(recommendation.Reasoning);

            context.Logger.Info($"Alert {alertId} triage complete: {recommendation.Disposition}",
                new Dictionary<string, object>
                {
                    {"disposition", recommendation.Disposition},
                    {"confidence", recommendation.Confidence}
                });

            return true;
        }
        catch (Exception ex)
        {
            context.Logger.Error($"Error in AlertTriageStage for alert {alertId}", ex);
            return false;
        }
    }
}
```

### Configuration

```xml
<!-- App.config / Web.config -->
<configuration>
  <appSettings>
    <add key="ConnectionString" value="data source=localhost;initial catalog=FCM;integrated security=True"/>
    <add key="FCM_HOME" value="C:\Program Files\FIS\FCM\"/>

    <!-- Redis Configuration -->
    <add key="Redis:ConnectionString" value="localhost:6379,ssl=false,abortConnect=false"/>
    <add key="Redis:Database" value="0"/>
    <add key="Redis:SessionTTL" value="01:00:00"/>
    <add key="Redis:CacheTTL" value="00:05:00"/>

    <!-- Azure OpenAI Configuration -->
    <add key="AzureOpenAI:Endpoint" value="https://your-openai.openai.azure.com/"/>
    <add key="AzureOpenAI:ApiKey" value="your-api-key"/>
    <add key="AzureOpenAI:DeploymentName" value="gpt-4"/>
    <add key="AzureOpenAI:EmbeddingDeployment" value="text-embedding-ada-002"/>

    <!-- Agent Configuration -->
    <add key="AlertTriage:MaxConcurrentAlerts" value="50"/>
    <add key="AlertTriage:AutoApplyConfidenceThreshold" value="0.85"/>
    <add key="AlertTriage:SimilarityThreshold" value="0.70"/>
    <add key="AlertTriage:TopKSimilar" value="10"/>
  </appSettings>

  <unity xmlns="http://schemas.microsoft.com/practices/2010/unity">
    <container>
      <!-- Repository Factory -->
      <register type="IRepositoryFactory" mapTo="RepositoryFactory">
        <lifetime type="singleton"/>
      </register>

      <!-- Redis -->
      <register type="IConnectionMultiplexer"
                mapTo="ConnectionMultiplexer"
                name="redis">
        <lifetime type="singleton"/>
        <constructor>
          <param name="configuration" value="localhost:6379"/>
        </constructor>
      </register>

      <!-- Semantic Kernel (configured in code) -->
      <register type="IKernel"
                mapTo="SemanticKernelBootstrapper"
                name="semanticKernel">
        <lifetime type="singleton"/>
      </register>

      <!-- Alert Triage Components -->
      <register type="IAlertTriageManager" mapTo="AlertTriageManager"/>
      <register type="AlertTriageOrchestrator"/>
      <register type="RedisSimilaritySearcher"/>
      <register type="RedisDecisionHistoryStore"/>
      <register type="RedisCriteriaCache"/>
    </container>
  </unity>
</configuration>
```

---

## Performance & Scaling

### Performance Targets

| Metric                  | Target           | Notes                     |
| ----------------------- | ---------------- | ------------------------- |
| Average Processing Time | < 2 seconds      | Per alert, end-to-end     |
| P95 Processing Time     | < 5 seconds      | 95th percentile           |
| P99 Processing Time     | < 10 seconds     | 99th percentile           |
| Throughput              | 500+ alerts/min  | Sustained load            |
| Peak Throughput         | 1000+ alerts/min | Burst capacity            |
| Redis Query Latency     | < 10ms           | P95 for cache reads       |
| Vector Search Latency   | < 100ms          | P95 for similarity search |
| LLM Response Time       | < 2 seconds      | Average for GPT-4         |
| Session Recovery Time   | < 30 seconds     | After failure             |

### Scaling Strategies

#### 1. Horizontal Scaling

```csharp
public class DistributedAlertTriageOrchestrator : AlertTriageOrchestrator
{
    private readonly string _instanceId;
    private readonly IDatabase _redis;

    public DistributedAlertTriageOrchestrator(
        IUnityContainer container,
        IRepositoryFactory factory)
        : base(container, factory)
    {
        _instanceId = $"agent_{Environment.MachineName}_{Guid.NewGuid():N}";
        _redis = container.Resolve<IConnectionMultiplexer>().GetDatabase();
    }

    protected override async Task<List<Alert>> GetPendingAlertsAsync()
    {
        // Use Redis queue for distributed work distribution
        var alertIds = await _redis.ListRightPopAsync("alerts:pending_queue", 10);

        if (alertIds.IsNullOrEmpty)
            return new List<Alert>();

        // Fetch alerts from database
        var alertRepo = _repositoryFactory.GetAlertRepository();
        var alerts = await alertRepo.GetByIdsAsync(
            alertIds.Select(x => int.Parse(x.ToString())).ToList());

        // Register ownership in Redis
        foreach (var alert in alerts)
        {
            await _redis.StringSetAsync(
                $"alert:owner:{alert.Id}",
                _instanceId,
                TimeSpan.FromMinutes(5));
        }

        return alerts.ToList();
    }

    public override async Task StopAsync()
    {
        // Release owned alerts back to queue
        var ownedAlerts = await GetOwnedAlertsAsync();

        foreach (var alertId in ownedAlerts)
        {
            await _redis.ListLeftPushAsync("alerts:pending_queue", alertId);
            await _redis.KeyDeleteAsync($"alert:owner:{alertId}");
        }

        await base.StopAsync();
    }
}
```

#### 2. Redis Clustering

```bash
# Redis Cluster Configuration (6 nodes: 3 masters, 3 replicas)
# redis-cluster.conf

port 7000
cluster-enabled yes
cluster-config-file nodes-7000.conf
cluster-node-timeout 5000
appendonly yes
appendfilename "appendonly-7000.aof"
maxmemory 2gb
maxmemory-policy allkeys-lru
```

```csharp
public class RedisClusterConfiguration
{
    public static ConnectionMultiplexer CreateClusterConnection()
    {
        var config = new ConfigurationOptions
        {
            EndPoints =
            {
                { "redis-node1", 7000 },
                { "redis-node2", 7001 },
                { "redis-node3", 7002 },
                { "redis-node4", 7003 },
                { "redis-node5", 7004 },
                { "redis-node6", 7005 }
            },
            ConnectTimeout = 5000,
            SyncTimeout = 5000,
            AbortOnConnectFail = false,
            AllowAdmin = true,
            KeepAlive = 60,
            ConnectRetry = 3
        };

        return ConnectionMultiplexer.Connect(config);
    }
}
```

#### 3. Caching Strategy

```csharp
public class MultiTierCacheStrategy
{
    private readonly IMemoryCache _l1Cache; // In-process memory
    private readonly IDatabase _l2Cache; // Redis
    private readonly IRepositoryFactory _l3Source; // Database

    public async Task<T> GetOrCreateAsync<T>(
        string key,
        Func<Task<T>> factory,
        TimeSpan l1Ttl,
        TimeSpan l2Ttl)
    {
        // L1: In-process memory cache
        if (_l1Cache.TryGetValue(key, out T value))
            return value;

        // L2: Redis cache
        var redisValue = await _l2Cache.StringGetAsync(key);
        if (!redisValue.IsNullOrEmpty)
        {
            value = JsonConvert.DeserializeObject<T>(redisValue);
            _l1Cache.Set(key, value, l1Ttl);
            return value;
        }

        // L3: Database (source of truth)
        value = await factory();

        // Populate caches
        await _l2Cache.StringSetAsync(
            key,
            JsonConvert.SerializeObject(value),
            l2Ttl);

        _l1Cache.Set(key, value, l1Ttl);

        return value;
    }
}
```

#### 4. Performance Monitoring

```csharp
public class PerformanceMonitor
{
    private readonly IDatabase _redis;
    private readonly ILogger _logger;

    public async Task MonitorPerformanceAsync()
    {
        while (true)
        {
            var metrics = await CollectMetricsAsync();

            // Check thresholds
            if (metrics.AvgProcessingTimeMs > 2000)
            {
                _logger.Warning("Average processing time exceeds target",
                    new Dictionary<string, object>
                    {
                        {"avg_time_ms", metrics.AvgProcessingTimeMs},
                        {"threshold_ms", 2000}
                    });
            }

            if (metrics.QueueDepth > 1000)
            {
                _logger.Warning("Alert queue depth high",
                    new Dictionary<string, object>
                    {
                        {"queue_depth", metrics.QueueDepth},
                        {"threshold", 1000}
                    });

                // Auto-scale trigger
                await TriggerAutoScaleAsync();
            }

            // Store metrics
            await StoreMetricsAsync(metrics);

            await Task.Delay(TimeSpan.FromSeconds(30));
        }
    }

    private async Task<SystemMetrics> CollectMetricsAsync()
    {
        var metrics = new SystemMetrics
        {
            Timestamp = DateTimeOffset.UtcNow,
            ActiveSessions = await _redis.SetLengthAsync("sessions:active"),
            QueueDepth = await _redis.ListLengthAsync("alerts:pending_queue"),
            CacheHitRate = await CalculateCacheHitRateAsync(),
            RedisMemoryUsed = await GetRedisMemoryUsageAsync()
        };

        // Get processing time stats from last hour
        var hourAgo = DateTimeOffset.UtcNow.AddHours(-1).ToUnixTimeMilliseconds();
        var recentProcessing = await _redis.StreamReadAsync(
            "metrics:dispositions",
            $"{hourAgo}-0",
            count: 10000);

        if (recentProcessing.Length > 0)
        {
            var times = recentProcessing
                .Select(e => (long)e.Values.First(v => v.Name == "processing_time_ms").Value)
                .ToList();

            metrics.AvgProcessingTimeMs = times.Average();
            metrics.P95ProcessingTimeMs = GetPercentile(times, 0.95);
            metrics.P99ProcessingTimeMs = GetPercentile(times, 0.99);
            metrics.ThroughputPerHour = recentProcessing.Length;
        }

        return metrics;
    }
}
```

---

## Summary

This detailed architecture provides:

1. **Robust Orchestration**: Supervisor pattern with graceful failure handling
2. **Intelligent Processing**: Semantic Kernel with custom plugins for FCM domain
3. **High-Performance Storage**: Redis with vector search, caching, and metrics
4. **Smart Decisions**: Multi-factor scoring with explainable AI reasoning
5. **Seamless Integration**: Works with existing FCM layers and patterns
6. **Production-Ready**: Monitoring, scaling, and performance optimization

The system learns from every decision, improving accuracy over time while maintaining full transparency and analyst control.
