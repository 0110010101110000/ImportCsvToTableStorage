using CommandLine;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ImportCsvToTableStorage
{
    class Program
    {
        static void Main(string[] args)
        {
            var result = Parser.Default.ParseArguments<Options>(args);
            var parsedOptions = result as Parsed<Options>;
            if (parsedOptions == null)
            {
                return;
            }
            if (!new[] { "insert", "insertorreplace", "insertormerge" }.Any(o => o.Equals(parsedOptions.Value.InsertMode.ToLower())))
            {
                parsedOptions.Value.InsertMode = "Insert";
                Console.WriteLine("Unsupported InsertMode, using Insert");
            }
            
            System.Diagnostics.Stopwatch sw1 = new System.Diagnostics.Stopwatch();
            sw1.Start();
            try
            {
                MainAsync(parsedOptions.Value).ContinueWith(c =>
                {
                    sw1.Stop();
                    Console.WriteLine($"Elapsed: {sw1.Elapsed}");
                }).GetAwaiter().GetResult();
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
        }

        static async Task MainAsync(Options options)
        {
            var filePath = options.InputCsvPath;
            Console.WriteLine($"reading {filePath}");
            var fileContent = File.ReadAllText(filePath);
            Console.WriteLine("DONE");
            var rows = fileContent.Split('\r').Where(r => !string.IsNullOrEmpty(r.Trim()));
            Console.WriteLine($"{rows.Count() - 1} rows");

            var headers = rows.First().Split(',');
            var colEntityPropertyFactories = headers.Select(_ => EntityPropertyFactory.Create("string")).ToArray();
            if (options.Types != null)
            {
                var types = options.Types.ToArray();
                for (var i = 0; i < types.Length; i++)
                {
                    colEntityPropertyFactories[i + 2] = EntityPropertyFactory.Create(types[i]);
                }
            }
            Console.WriteLine($"{rows.Count() - 1} rows");
            var entities = rows.Skip(1).Select(r =>
            {
                var cols = r.Split(',');
                var dict = new Dictionary<string, EntityProperty>();
                for (var i = 2; i < cols.Length; i++)
                {
                    //dict.Add(headers[i], new EntityProperty(cols[i].Trim()));                    
                    dict.Add(headers[i], colEntityPropertyFactories[i](cols[i].Trim()));
                }
                return new DynamicTableEntity(cols[0].Trim(), cols[1].Trim(), null, dict);
            }).ToArray();

            var cloudTableClient = new CloudTableClient(
                new Uri(string.Format("https://{0}.table.core.windows.net", options.StorageAccountName)),
                new StorageCredentials(options.StorageAccountName, options.StorageAccountKey)
            );

            cloudTableClient.DefaultRequestOptions.PayloadFormat = TablePayloadFormat.JsonNoMetadata;
            cloudTableClient.DefaultRequestOptions.RetryPolicy = new LinearRetry(new TimeSpan(0,0,1), 2);
            
            var table = cloudTableClient.GetTableReference(options.TableName);
                       
            var tasks = new List<Task>();
            var batchExecute = BatchExecuteFactory.Create(options.InsertMode);

            //var batchPaging = 0;
            //var batchSize = 100000;
            //do
            //{
            //    Console.WriteLine($"BATCH {batchPaging + 1} - FROM {batchSize * batchPaging + 1} to {batchSize * (batchPaging + 1)}");
            //entities.Skip(batchSize * batchPaging).Take(batchSize).GroupBy(e => e.PartitionKey).AsParallel().ForAll(g =>
            entities.GroupBy(e => e.PartitionKey).AsParallel().ForAll(g =>
            //    foreach(var g in entities.GroupBy(e => e.PartitionKey))
                {
                    Console.WriteLine($"Batch for partition {g.Key} {g.Count()} records");
                    tasks.Add(BatchInsertAsync(table, g.ToList(), batchExecute));
                }
             );

                try
                {
                    await Task.WhenAll(tasks);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
                //Console.WriteLine("COMPLETE");
            //    batchPaging++;
            //}
            //while (batchPaging * batchSize < entities.Count());
        }

        static async Task BatchInsertAsync(CloudTable table, List<DynamicTableEntity> entities, Action<TableBatchOperation, ITableEntity> batchExecute)
        {
            int rowOffset = 0;
            var tasks = new List<Task>();
            while (rowOffset < entities.Count)
            {
                // next batch
                var rows = entities.Skip(rowOffset).Take(100).ToList();
                rowOffset += rows.Count;
                var batch = new TableBatchOperation();
                
                foreach (var row in rows)
                {
                    batchExecute(batch, row);
                }
                // submit
                Console.WriteLine($"Start batch\tpartition {rows.First().PartitionKey}\t | {rows.First().RowKey} \t | {rows.Last().RowKey}");
                tasks.Add(table.ExecuteBatchAsync(batch).ContinueWith(t => { Console.WriteLine($"End batch\t partition {rows.First().PartitionKey}\t | {rows.First().RowKey}\t | {rows.Last().RowKey} \t| {!t.IsFaulted}"); }));
            }
            await Task.WhenAll(tasks);
//            Console.WriteLine("##COMPLETE##");
        }
    }

    internal static class BatchExecuteFactory
    {
        internal static Action<TableBatchOperation, ITableEntity> Create(string insertMode)
        {
            switch (insertMode.ToLower())
            {
                case "insertorreplace":
                    return (tbo, te) => tbo.InsertOrReplace(te);

                case "insertormerge":
                    return (tbo, te) => tbo.InsertOrMerge(te);

                case "insert":
                    return (tbo, te) => tbo.Insert(te);
                default: throw new NotImplementedException($"{insertMode}");
            }
        }
    }

    internal static class EntityPropertyFactory
    {
        internal static Func<string, EntityProperty> Create(string type)
        {

            //binary,bool|boolean,datetime,double,guid,int|int32,long|int64,string
            switch (type.ToLower())
            {
                case "binary": return (string value) => new EntityProperty(Convert.FromBase64String(value));
                case "boolean":
                case "bool": return (string value) => string.IsNullOrEmpty(value) ? new EntityProperty(default(bool?)) : new EntityProperty((bool)Convert.ChangeType(value, typeof(bool)));
                case "guid": return (string value) => string.IsNullOrEmpty(value) ? new EntityProperty(default(Guid?)) : new EntityProperty(new Guid(value));
                case "datetime": return (string value) => string.IsNullOrEmpty(value) ? new EntityProperty(default(DateTime?)) : new EntityProperty((DateTime)Convert.ChangeType(value, typeof(DateTime)));
                case "int":
                case "int32": return (string value) => string.IsNullOrEmpty(value) ? new EntityProperty(default(int?)) : new EntityProperty((int)Convert.ChangeType(value, typeof(int)));
                case "long":
                case "int64": return (string value) => string.IsNullOrEmpty(value) ? new EntityProperty(default(long?)) : new EntityProperty((long)Convert.ChangeType(value, typeof(long)));
                case "double": return (string value) => string.IsNullOrEmpty(value) ? new EntityProperty(default(double?)) : new EntityProperty((double)Convert.ChangeType(value, typeof(double)));
                case "string": return (string value) => string.IsNullOrEmpty(value) ? new EntityProperty(default(string)) : new EntityProperty(value);
            }
            throw new NotImplementedException($"{type} is not supported");
        }
    }

    internal class Options
    {
        [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
        public bool Verbose { get; set; }

        [Option('t', "table-name", Required = true, HelpText = "Name of the Storage Table to import to - must exist")]
        public string TableName { get; set; }

        [Option('n', "storage-account-name", Required = true, HelpText = "")]
        public string StorageAccountName { get; set; }

        [Option('k', "key", Required = true, HelpText = "The Storage account key")]
        public string StorageAccountKey { get; set; }

        [Option('i', "input-csv-path", Required = true, HelpText = "absolute path to the csv file to import")]
        public string InputCsvPath { get; set; }

        [Option('m', "insert-mode", Required = false, Default = "insert", HelpText = "insert mode for the batch operation. one of: Insert | InsertOrReplace | InsertOrMerge. Default is Insert")]
        public string InsertMode { get; set; }

        [Option("types", Separator = ',', Required = false, HelpText = "A comma separated list of the types for the data columns (excluding the first two columns that are reserved for partition and row keys). Valid values are: binary,bool|boolean,datetime,double,guid,int|int32,long|int64,string")]
        public IEnumerable<string> Types { get; set; }
    }
}
