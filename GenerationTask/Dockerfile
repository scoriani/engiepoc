FROM microsoft/dotnet:2.0-runtime
WORKDIR /app

# copy csproj and restore as distinct layers
COPY *.csproj ./
#RUN dotnet restore

# copy and build everything else
COPY . ./
#RUN dotnet publish -c Release -o out
ENTRYPOINT ["dotnet", "/app/out/Generation.dll"]