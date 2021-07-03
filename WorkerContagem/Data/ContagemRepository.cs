using System;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Dapper.Contrib.Extensions;
using WorkerContagem.Models;

namespace WorkerContagem.Data
{
    public class ContagemRepository
    {
        private readonly IConfiguration _configuration;
        private readonly TimeZoneInfo _timeZoneBrasil;

        public ContagemRepository(IConfiguration configuration)
        {
            _configuration = configuration;
            _timeZoneBrasil = TimeZoneInfo.FindSystemTimeZoneById(
                "E. South America Standard Time");
        }

        public void Save(ResultadoContador resultado)
        {
            using var conexao = new SqlConnection(
                _configuration.GetConnectionString("BaseContagem"));
            conexao.Insert<HistoricoContagem>(new ()
            {
                DataProcessamento = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, _timeZoneBrasil),
                ValorAtual = resultado.ValorAtual,
                Producer = resultado.Producer,
                Consumer = Environment.MachineName,
                Topico = _configuration["ApacheKafka:Topic"],
                Mensagem = resultado.Mensagem,
                Kernel = resultado.Kernel,
                TargetFramework = resultado.TargetFramework
            });
        }
    }
}