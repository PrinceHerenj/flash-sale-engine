using OrderWorkerFleet;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<OrderWorker>();

var host = builder.Build();
host.Run();
