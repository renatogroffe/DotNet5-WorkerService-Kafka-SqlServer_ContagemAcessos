using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using WorkerContagem.Data;
using WorkerContagem.Models;

namespace WorkerContagem
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        private readonly ContagemRepository _repository;
        private readonly string _topico;
        private readonly string _groupId;
        private readonly IConsumer<Ignore, string> _consumer;

        public Worker(ILogger<Worker> logger,
            IConfiguration configuration,
            ContagemRepository repository)
        {
            _logger = logger;
            _configuration = configuration;
            _repository = repository;

            _topico = _configuration["ApacheKafka:Topic"];
            _groupId = _configuration["ApacheKafka:GroupId"];

            _consumer = new ConsumerBuilder<Ignore, string>(
                new ConsumerConfig()
                {
                    BootstrapServers = configuration["ApacheKafka:Host"],
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    SaslMechanism = SaslMechanism.Plain,
                    SaslUsername = configuration["ApacheKafka:Username"],
                    SaslPassword = configuration["ApacheKafka:Password"],
                    GroupId = _groupId,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                }).Build();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"Topic = {_topico}");
            _logger.LogInformation($"Group Id = {_groupId}");
            _logger.LogInformation("Aguardando mensagens...");
            _consumer.Subscribe(_topico);

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Run(() =>
                {
                    var result = _consumer.Consume(stoppingToken);
                    var dadosContagem = result.Message.Value;
                    
                    _logger.LogInformation(
                        $"[{_topico} | {_groupId} | Nova mensagem] " +
                        dadosContagem);

                    ProcessarResultado(dadosContagem);
                });
           }
        }

        private void ProcessarResultado(string dados)
        {
            ResultadoContador resultado;            
            try
            {
                resultado = JsonSerializer.Deserialize<ResultadoContador>(dados,
                    new JsonSerializerOptions()
                    {
                        PropertyNameCaseInsensitive = true
                    });
            }
            catch
            {
                _logger.LogError("Dados inválidos para o Resultado");
                resultado = null;
            }

            if (resultado is not null)
            {
                _repository.Save(resultado);
                _logger.LogInformation("Resultado registrado com sucesso!");
            }
        }
    }
}