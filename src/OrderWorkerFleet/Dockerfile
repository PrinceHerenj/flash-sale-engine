FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .
RUN dotnet restore src/OrderWorkerFleet/OrderWorkerFleet.csproj
RUN dotnet publish src/OrderWorkerFleet/OrderWorkerFleet.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "OrderWorkerFleet.dll"]
