using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Transactions;

namespace ReadingAndSavingFileJSON
{
    class Program
    {
        static StringBuilder sb = new StringBuilder();
        static void Main(string[] args)
        {
            DateTime startTempo = DateTime.Now;
            string strConexao = @"Data Source=GTI-15\PRICEFY;Initial Catalog=pricefy;Integrated Security=True;";
            //string strConexao = @"Data Source=JAIR\SQLEXPRESS; Initial Catalog=pricefy; Integrated Security=true;";            
            string arquivoCSVtPath = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName + @"\data\ExemploPriceFy.csv";            

            Console.WriteLine("");            
            Console.WriteLine("                  IMPORTACAO ARQUIVO .CSV - PRICEFY ");
            Console.WriteLine("=======================================================================");
            Console.WriteLine("");
            Console.WriteLine("");
            Console.WriteLine("[1/4] INICIANDO PROCESSO DE IMPORTACAO");            
            Console.WriteLine("-----------------------------------------------------------------------");
            var arquivoLocalizado = File.Exists(arquivoCSVtPath);
            Console.WriteLine("Situacao da arquivo.......: " + (arquivoLocalizado ? "Lendo..." : "Não existente"));
            var linhas = arquivoLocalizado ? File.ReadAllLines(arquivoCSVtPath) : new String[0];
            Console.WriteLine("Consistencia do arquivo...: " + (linhas.Count() > 0 ? "Válido OK" : "Vazio/inválido"));
            Console.WriteLine("Tempo de processamento....: {0} segundos", Math.Round( DateTime.Now.Subtract(startTempo).TotalSeconds) );
            Console.WriteLine("Linhas do arquivos........: {0}", linhas.Count() -1);
            if (linhas.Count() == 0 || !arquivoLocalizado)
            {
                Console.ReadKey();
                return;
            }
           
            using (var transactionScope = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(strConexao))
                {
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandTimeout = TimeSpan.FromMinutes(300).Seconds;
                        try
                        {
                            Console.WriteLine("Conectando bade de dados..: iniciando...");
                            conn.Open();
                            Console.WriteLine("Conexao...................: OK");
                            try
                            {
                                Console.WriteLine("-----------------------------------------------------------------------");
                                Console.WriteLine("");
                                Console.WriteLine("[2/4]  QUESTIONÁRIO");
                                Console.Write("-----------------------------------------------------------------------");
                                Console.Write("\nINFORME O DELIMITADOR DOS CAMPOS(<enter> default ';')........: ");
                                 
                                var delimitadorCampos =  Console.ReadLine();
                                delimitadorCampos = delimitadorCampos == "" ? ";" : delimitadorCampos;
                                Console.Write("Opcao informada...........: '" + delimitadorCampos + "'");

                                var campos = linhas[0].Split( Convert.ToChar( delimitadorCampos ));

                                Console.Write("\nINFORME A TABELA MESTRE(<enter> default: TbCargaCSV).........: ");
                                var tabelaMestre      = Console.ReadLine();
                                tabelaMestre = tabelaMestre == "" ? "TbCargaCSV" : tabelaMestre;
                                Console.Write("Opcao informada...........: '" + tabelaMestre + "'");                                
                                Console.Write("\nINFORME A TABELA DETALHE(<enter> default: TbCargaDetalheCSV).: ");
                                var tabelaDetalhe = Console.ReadLine(); ;
                                tabelaDetalhe = tabelaDetalhe == "" ? "TbCargaDetalheCSV" : tabelaDetalhe;
                                Console.WriteLine("Opcao imformada.......: '" + tabelaDetalhe + "'");
                                Console.WriteLine("-----------------------------------------------------------------------");
                                Console.WriteLine("");
                               
                                Console.WriteLine("[3/4]  REALIZANDO A LEITURA/IMPORTACAO .CSV");
                                Console.WriteLine("-----------------------------------------------------------------------");
                                Console.WriteLine("Preparacao do ambiente....: iniciando...");
                                startTempo = DateTime.Now;                         
                                // campos dinamicos da tabela
                                PrepararTabelasCarga(campos, cmd, tabelaMestre, tabelaDetalhe);
                                Console.WriteLine("Ambiente..................: OK");                                
                                Console.WriteLine("Realizando a carga agora..: iniciando...");
                                string[] resultado = RealizarCarga(arquivoCSVtPath, cmd, tabelaMestre, tabelaDetalhe, delimitadorCampos);
                                transactionScope.Complete();
                                Console.WriteLine("Carga/importação..........: OK");
                                Console.WriteLine("-----------------------------------------------------------------------");
                                Console.WriteLine("");

                                Console.WriteLine("[4/4]  RESUMO DA IMPORTAÇÃO - FINAL");
                                Console.WriteLine("-----------------------------------------------------------------------");
                                Console.WriteLine("Nome do aquivo............: {0}", resultado[0]);
                                Console.WriteLine("ID(Código da carga).......: {0}", resultado[1]);
                                Console.WriteLine("Linhas inseridas..........: {0}", resultado[2]);
                                Console.WriteLine("Tempo de processamento....: {0} segundos", Math.Round(DateTime.Now.Subtract(startTempo).TotalSeconds));
                                Console.WriteLine("");
                                Console.WriteLine("");
                                Console.WriteLine("=======================================================================");
                                Console.WriteLine("                       API - TESTE DA IMPORTACAO");
                                Console.WriteLine("=======================================================================");
                                Console.WriteLine("baseurl...................: http//localhost/api/carga");
                                Console.WriteLine("id........................: Id da carga. Pode ser por carga especifica");
                                Console.WriteLine("pagina....................: paginação especifica");
                                Console.WriteLine("limite....................: limite maximo de registro por pagina");
                                Console.WriteLine("");
                                Console.WriteLine("                           ### EXEMPLO ###");
                                Console.WriteLine("http//localhost/api/importacao/paginacao?numeroPagina=1&LimitePagina=20");
                                Console.WriteLine("-----------------------------------------------------------------------");
                                Console.ReadKey();                                
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("");
                                Console.WriteLine("================================================");
                                Console.WriteLine("            |        ERRO        |             ");
                                Console.WriteLine(ex.Message);
                                Console.WriteLine("================================================");
                                Console.ReadKey();
                            };
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("");
                            Console.WriteLine("================================================");
                            Console.WriteLine("            |        ERRO        |             ");
                            Console.WriteLine(ex.Message);
                            Console.WriteLine("================================================");
                            Console.ReadKey();
                        };
                    }
                }
            }
        }
        static void PrepararTabelasCarga(string[] campos, SqlCommand cmd, string tabelaMestre, string tabelaDetalhe)
        {         
            // cria a tabela Mestre com colunas fixas
            sb.Clear();
            sb.Append("IF OBJECT_ID('dbo." + tabelaMestre + "', 'U') IS NOT NULL" );
            sb.Append("   DROP TABLE [dbo]." + tabelaMestre                        );
            sb.Append("   CREATE TABLE [dbo]." + tabelaMestre                      );
            sb.Append("   (   id_carga      INT NOT NULL IDENTITY(1,1) PRIMARY KEY");
            sb.Append("      ,nm_arquivo    VARCHAR(50)"                           );
            sb.Append("      ,dt_carga      DATETIME DEFAULT GETDATE()"            );            
            sb.Append("      ,nr_registros  INT"                                   );
            sb.Append("   );"                                                      );
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();

            // cria os campos dinamicamente
            System.Text.RegularExpressions.Regex reg = new System.Text.RegularExpressions.Regex("[*'\",&#^@´`-]");
            StringBuilder sbAuxCampos = new StringBuilder();
            int i = 0;
            foreach (var campo in campos)
            {
                i++;
                var campoSemCharEspecial = reg.Replace(campo.Replace(" ","_"), string.Empty);
                sbAuxCampos.Append((i==1 ? " " : ", ") + campoSemCharEspecial + " VARCHAR(255) ");
            };
            sb.Clear();
            sb.Append("IF OBJECT_ID('dbo." + tabelaDetalhe + "', 'U') IS NOT NULL" );
            sb.Append("   DROP TABLE [dbo]." + tabelaDetalhe);            
            sb.Append("   CREATE TABLE [dbo]." + tabelaDetalhe);
            sb.Append("   (  "                                                 );
          //sb.Append("   (  id         INT NOT NULL IDENTITY(1,1) PRIMARY KEY");
          //sb.Append("     ,id_carga   INT"                                   );
            sb.Append(      sbAuxCampos.ToString()                             );
            sb.Append("   );"                                                  );
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        static string[] RealizarCarga(string arquivoCSVPath, SqlCommand cmd, string tabelaMestre, string tabelaDetalhe, string delimitadorCampos)
        {
            // realizando a carga e removendo o primeiro registro(este é também o nome dos campos
            sb.Clear();
            sb.Append(" bulk insert [dbo].[" + tabelaDetalhe + "]"                            );
            sb.Append(" from '" + arquivoCSVPath + "'"                                        );
            sb.Append(" with (rowterminator = '\\n', fieldterminator = '" + delimitadorCampos + "', CODEPAGE = 'RAW') ");
            sb.Append(" delete top(1) from [dbo].[" + tabelaDetalhe + "]");
            cmd.CommandText = sb.ToString();
            int linhasAfetadas = cmd.ExecuteNonQuery();

            string nomeArquivo = Path.GetFileName(arquivoCSVPath);
            // 1 - id       poderá ser automatico
            // 2 - dt_carga poderá ser automatica
            sb.Clear();
            sb.Append(" INSERT INTO [dbo].[" + tabelaMestre + "]"     );
            sb.Append("             ( nm_arquivo"           );
            sb.Append("              ,nr_registros"         );
            sb.Append("             )"                      );
            sb.Append("      OUTPUT INSERTED.ID_CARGA"      );
            sb.Append("      VALUES ('" + nomeArquivo + "'" );
            sb.Append("              ," + linhasAfetadas    );
            sb.Append("             )"                      );
            sb.Append("   ");
            cmd.CommandText = sb.ToString();
            int id_carga = cmd.ExecuteNonQuery();
            
            //sb.Clear();
            //sb.Append(" UPDATE [dbo].[TbCargaDetalheCSV]");
            //sb.Append("    SET id_carga = " + id_carga);
            //cmd.CommandText = sb.ToString();
            //cmd.ExecuteNonQuery();

            string[] result = { nomeArquivo, id_carga.ToString(), (linhasAfetadas - 1).ToString() };
            
            return result;
        }    
    }
}
