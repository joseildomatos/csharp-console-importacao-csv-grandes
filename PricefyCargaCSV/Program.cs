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
            //string strConexao = @"Data Source=NOTEBOOK\PRICEFY; Initial Catalog=pricefy; Integrated Security=true;";            
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
            Console.WriteLine("Tempo de processamento....: " + (Math.Round(DateTime.Now.Subtract(startTempo).TotalSeconds) > 60 ? Math.Round((DateTime.Now.Subtract(startTempo).TotalSeconds) / 60) + " minutos" : Math.Round(DateTime.Now.Subtract(startTempo).TotalSeconds) +" segundos"));
            Console.WriteLine("Linhas do arquivos........: " + (linhas.Count() - 1));
            if (linhas.Count() == 0 || !arquivoLocalizado)
            {
                Console.ReadKey();
                return;
            }
            using (var conn = new SqlConnection(strConexao))
            {
                conn.Open();
                using (var cmd = new SqlCommand())
                {
                    cmd.Connection     = conn;
                    cmd.CommandTimeout = 3000;
                    try
                    {
                        Console.WriteLine("Conectando bade de dados..: iniciando...");
                        Console.WriteLine("Conexao...................: OK");
                        try
                        {
                            Console.WriteLine("-----------------------------------------------------------------------");
                            Console.WriteLine("");
                            Console.WriteLine("[2/4]  QUESTIONÁRIO");
                            Console.Write("-----------------------------------------------------------------------");
                            Console.Write("\nINFORME O DELIMITADOR DOS CAMPOS(<enter> default ';')........: ");

                            var delimitadorCampos = Console.ReadLine();
                            delimitadorCampos = delimitadorCampos == "" ? ";" : delimitadorCampos;
                            Console.WriteLine("Opcao informada...........: '" + delimitadorCampos + "'");
                            /*
                            Console.Write("\nINFORME A TABELA MESTRE(<enter> default: TbCargaCSV).........: ");
                            var tabelaMestre = Console.ReadLine();
                            tabelaMestre = tabelaMestre == "" ? "TbCargaCSV" : tabelaMestre;
                            Console.Write("Opcao informada...........: '" + tabelaMestre + "'");
                            Console.Write("\nINFORME A TABELA DETALHE(<enter> default: TbCargaDetalheCSV).: ");
                            var tabelaDetalhe = Console.ReadLine();
                            tabelaDetalhe = tabelaDetalhe == "" ? "TbCargaDetalheCSV" : tabelaDetalhe;
                            */
                            var tabelaMestre  = "TbCargaCSV";
                            var tabelaDetalhe = "TbCargaDetalheCSV";
                            /*
                            Console.WriteLine("Opcao imformada.......: '" + tabelaDetalhe + "'");                            
                            Console.WriteLine("-----------------------------------------------------------------------");                            
                            */
                            Console.WriteLine("");
                            Console.WriteLine("[3/4]  REALIZANDO A LEITURA/IMPORTACAO .CSV");
                            Console.WriteLine("-----------------------------------------------------------------------");
                            Console.WriteLine("Preparacao do ambiente....: iniciando...");
                            startTempo = DateTime.Now;

                            // campos dinamicos da tabela
                            var campos = linhas[0].Split(Convert.ToChar(delimitadorCampos));
                            PrepararTabelasCarga(campos, cmd, tabelaMestre, tabelaDetalhe);
                            Console.WriteLine("Ambiente..................: OK");
                            Console.WriteLine("Realizando a carga agora..: iniciando...");

                            var resultado = RealizarCarga(arquivoCSVtPath, cmd, tabelaMestre, tabelaDetalhe, delimitadorCampos);
                            Console.WriteLine("Carga/importação..........: OK");
                            Console.WriteLine("-----------------------------------------------------------------------");
                            Console.WriteLine("");
                            Console.WriteLine("[4/4]  RESUMO DA IMPORTAÇÃO - FINAL");
                            Console.WriteLine("-----------------------------------------------------------------------");
                            Console.WriteLine("Nome do aquivo............: {0}", resultado[0]);
                            Console.WriteLine("ID(Código da carga).......: {0}", resultado[1]);
                            Console.WriteLine("Linhas inseridas..........: {0}", (int)resultado[2]);
                            Console.WriteLine("Tempo de processamento....: " + (Math.Round(DateTime.Now.Subtract(startTempo).TotalSeconds) > 60 ? Math.Round((DateTime.Now.Subtract(startTempo).TotalSeconds) / 60) + " minutos" : Math.Round(DateTime.Now.Subtract(startTempo).TotalSeconds) + " segundos"));
                            Console.WriteLine("");
                            Console.WriteLine("");
                            Console.WriteLine("=======================================================================");
                            Console.WriteLine("                       API - TESTE DA IMPORTACAO");
                            Console.WriteLine("=======================================================================");
                            Console.WriteLine("baseurl...................: http//localhost:{porta}/api/importacao/paginacao");
                            //Console.WriteLine("id........................: Id da carga. Pode ser por carga especifica");
                            Console.WriteLine("numeroPagina..............: paginação especifica");
                            Console.WriteLine("limitePagina..............: limite maximo de registro por pagina");
                            Console.WriteLine("");
                            Console.WriteLine("");
                            Console.WriteLine("                           ### EXEMPLO ###");
                            Console.WriteLine("http//localhost:{porta}/api/importacao/paginacao?numeroPagina=1&LimitePagina=20");
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
            string campoSemCharEspecial = "", campoIndice = "", camposTabela = "";
            foreach (var campo in campos)
            {
                i++;
                campoSemCharEspecial = reg.Replace(campo.Replace(" ","_"), string.Empty);                
                sbAuxCampos.Append((i==1 ? " " : ", ") + campoSemCharEspecial + " VARCHAR(50) ");
                if (i<=2)
                    campoIndice  += campoSemCharEspecial + " ASC " + (i == 1 ? "," : "");
                if (i>=3)
                    camposTabela += (i == 3 ? " " : ", ") + campoSemCharEspecial;
            };
            sb.Clear();
            sb.Append("IF OBJECT_ID('dbo." + tabelaDetalhe + "', 'U') IS NOT NULL" );
            sb.Append("   DROP TABLE [dbo]." + tabelaDetalhe + ";");            
            sb.Append("   CREATE TABLE [dbo]." + tabelaDetalhe);
            sb.Append("   (  "                                                 );
          //sb.Append("      id         INT NOT NULL IDENTITY(1,1) PRIMARY KEY");
          //sb.Append("     ,id_carga   INT"                                   );
            sb.Append(      sbAuxCampos.ToString()                             );
            sb.Append("   );"                                                  );
            sb.Append("   CREATE NONCLUSTERED INDEX [idx_pricefy] ON [dbo].[" + tabelaDetalhe + "]");
            sb.Append("   (" + campoIndice  + ")");
            sb.Append("   INCLUDE (" + camposTabela + ");");            
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        static object[] RealizarCarga(string arquivoCSVPath, SqlCommand cmd, string tabelaMestre, string tabelaDetalhe, string delimitadorCampos)
        {
            // realizando a carga e removendo o primeiro registro(este é também o nome dos campos
            sb.Clear();
            sb.Append(" bulk insert [dbo].[" + tabelaDetalhe + "]"             );
            sb.Append(" from '" + arquivoCSVPath + "'"                         );
            sb.Append(" with ( "                                               );
            sb.Append("        rowterminator   = '\\n', "                      );
            sb.Append("        fieldterminator = '" + delimitadorCampos + "'," );
            sb.Append("        codepage        = 'RAW', "                      );
            sb.Append("        firstrow        = 2"                            );
            sb.Append("      ) ;"                                              );
            cmd.CommandText = sb.ToString();
            int linhasAfetadas = cmd.ExecuteNonQuery();

            string nomeArquivo = Path.GetFileName(arquivoCSVPath);
            // 1 - id       é automatico
            // 2 - dt_carga é automatica
            sb.Clear();
            sb.Append(" INSERT INTO [dbo].[" + tabelaMestre + "]"     );
            sb.Append("             ( nm_arquivo"           );
            sb.Append("              ,nr_registros"         );
            sb.Append("             )"                      );
            sb.Append("      OUTPUT INSERTED.ID_CARGA"      );
            sb.Append("      VALUES ('" + nomeArquivo + "'" );
            sb.Append("              ," + linhasAfetadas.ToString() );
            sb.Append("             )"                      );
            sb.Append("   ");
            cmd.CommandText = sb.ToString();
            int id_carga = cmd.ExecuteNonQuery();
            //sb.Clear();
            //sb.Append(" UPDATE [dbo].[TbCargaDetalheCSV]");
            //sb.Append("    SET id_carga = " + id_carga);
            //cmd.CommandText = sb.ToString();
            //cmd.ExecuteNonQuery();
            object[] result = { nomeArquivo, id_carga.ToString(), linhasAfetadas };            
            return result;
        }    
    }
}
