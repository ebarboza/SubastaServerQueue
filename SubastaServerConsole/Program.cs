using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SubastaServerConsole
{
    class Program
    {
        static string strConAzure = "";
        static string queueNameAzure = "productosqueue";
        static string accessKeyAWS = "";
        static string secretKeyAWS = "";
        static string serviceURLAWS = "";


        static void Main(string[] args)
        {
            LimpiarColaSubastas();
            InicializarColaSubasta();

            List<Articulo> historicoPujas = new List<Articulo>();
            List<Articulo> mejoresPujas = new List<Articulo>();

            //Leer pujas y validar la mejor
            while (true)
            {
                var nuevaPuja = LeerPuja();

                if (nuevaPuja != null && nuevaPuja.IdArticulo != null)
                {
                    historicoPujas.Add(nuevaPuja);

                    var nuevaMejorPuja = historicoPujas.Where(c => c.IdArticulo == nuevaPuja.IdArticulo).OrderByDescending(a => a.MontoOfrecido).First();
                    var mejorPujaActual = mejoresPujas.Where(c => c.IdArticulo == nuevaPuja.IdArticulo).FirstOrDefault<Articulo>();

                    if (mejorPujaActual == null)
                    {
                        mejoresPujas.Add(nuevaPuja);
                        Console.WriteLine(string.Format("Nueva mejor puja: Articulo Id {0} - Monto:{1}", nuevaPuja.IdArticulo, nuevaPuja.MontoOfrecido));
                    }
                    else
                    {
                        if (nuevaMejorPuja.MontoOfrecido >= mejorPujaActual.MontoOfrecido)
                        {
                            mejoresPujas.Remove(mejorPujaActual);
                            mejoresPujas.Add(nuevaMejorPuja);

                            Console.WriteLine(string.Format("Nueva mejor puja: Articulo Id {0} - Monto:{1}", nuevaMejorPuja.IdArticulo, nuevaMejorPuja.MontoOfrecido));
                        }
                    }
                }
            }
        }

        public static void InicializarColaSubasta()
        {
            var cliente = QueueClient.CreateFromConnectionString(strConAzure, queueNameAzure);

            var msg = new BrokeredMessage();
            DateTime now = DateTime.Now;

            List<Articulo> articulosSubasta = new List<Articulo>();

            articulosSubasta.Add(new Articulo
            {
                IdArticulo = "1",
                Nombre = "Reloj Star Wars",
                MontoOfrecido = 0,
                Fecha = now
            });

            foreach (var subasta in articulosSubasta)
            {
                msg.Properties.Add("IdArticulo", subasta.IdArticulo);
                msg.Properties.Add("Nombre", subasta.Nombre);
                msg.Properties.Add("MontoOfrecido", subasta.MontoOfrecido);
                msg.Properties.Add("Fecha", subasta.Fecha);

                cliente.Send(msg);
            }
        }

        public static void LimpiarColaSubastas()
        {
            var cliente = QueueClient.CreateFromConnectionString(strConAzure, queueNameAzure);

            BrokeredMessage brokeredMessage = cliente.Receive();

            while (brokeredMessage != null)
            {
                brokeredMessage.Complete();
                brokeredMessage = cliente.Receive();
            } 
        }

        public static Articulo LeerPuja()
        {            
            var awsCred = new BasicAWSCredentials(accessKeyAWS, secretKeyAWS);
            IAmazonSQS amazonSqsClient = new AmazonSQSClient(awsCred, RegionEndpoint.USWest2);

            var receiveMessageRequest = new ReceiveMessageRequest();
            receiveMessageRequest.QueueUrl = serviceURLAWS;
            var pujas = new Articulo();

            var responseFromQueue = amazonSqsClient.ReceiveMessageAsync(receiveMessageRequest).Result;

            if (responseFromQueue.Messages.Any())
            {
                foreach (var message in responseFromQueue.Messages)
                {
                    var deleteMessageRequest = new DeleteMessageRequest();
                    deleteMessageRequest.QueueUrl = serviceURLAWS;
                    deleteMessageRequest.ReceiptHandle = message.ReceiptHandle;

                    var result = amazonSqsClient.DeleteMessageAsync(deleteMessageRequest).Result;

                    pujas = Deserialize<Articulo>(message.Body, true);
                }
            }
            return pujas;
        }

        public static T Deserialize<T>(string json, bool ignoreMissingMembersInObject) where T : class
        {
            T deserializedObject;
            try
            {
                MissingMemberHandling missingMemberHandling = MissingMemberHandling.Error;
                if (ignoreMissingMembersInObject)
                    missingMemberHandling = MissingMemberHandling.Ignore;
                deserializedObject = JsonConvert.DeserializeObject<T>(json, new JsonSerializerSettings
                {
                    MissingMemberHandling = missingMemberHandling,
                });
            }
            catch (JsonSerializationException)
            {
                return null;
            }
            return deserializedObject;
        }
    }

    public class Articulo
    {
        public string IdArticulo { set; get; }
        public string Nombre { set; get; }
        public double MontoOfrecido { set; get; }
        public DateTime Fecha { set; get; }
    }
}
