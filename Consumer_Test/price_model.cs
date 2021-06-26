using Avro;
using Avro.Specific;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace io.confluent.ksql.avro_schemas
{
    public class TestModel
    {
        public decimal UNITPRICE { get; set; }
        public decimal totalMoney { get; set; }

        public decimal totalCount { get; set; }

        public override string ToString()
        {
			return $"TotalMoney : {totalMoney}, totalcount : {totalCount}, unitprice: {UNITPRICE}";
        }
    }

	public class KsqlDataSourceSchema : ISpecificRecord
	{
		//public static Schema _SCHEMA = Schema.Parse("{\"type\":\"record\",\"name\":\"Item\",\"namespace\":\"MyTestDemo.Model\",\"fields\":[{\"name\":\"Id\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"},{\"name\":\"ItemCount\",\"type" +
		//"\":\"float\"},{\"name\":\"TotalMoney\",\"type\":\"float\"}]}");

		//public static Schema _SCHEMA = Schema.Parse("{\"type\":\"record\",\"name\":\"Value\",\"namespace\":\"ca6340e403ac.dbo.Items\",\"fields\":[{\"name\":\"Id\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"},{\"name\":\"ItemCount\",\"type\":\"double\"},{\"name\":\"TotalMoney\",\"type\":\"double\"},{\"name\":\"__deleted\",\"default\":null,\"type\":[\"null\",\"string\"]}]}");

		//public static Schema _SCHEMA = Schema.Parse("{\"type\":\"record\",\"name\":\"KsqlDataSourceSchema\",\"namespace\":\"io.confluent.ksql.avro_schemas\",\"fields\":[{\"name\":\"TOTALMONEY\",\"default\":null,\"type\":[\"null\",\"bytes\"]},{\"name\":\"TOTALCOUNT\",\"default\":null,\"type\":[\"null\",\"int\"]},{\"name\":\"UNITPRICE\",\"default\":null,\"type\":[\"null\",\"bytes\"]}]}");

		public static Schema _SCHEMA = Schema.Parse("{\"type\":\"record\",\"name\":\"KsqlDataSourceSchema\",\"namespace\":\"io.confluent.ksql.avro_schemas\",\"fields\":[{\"name\":\"TOTALMONEY\",\"type\":\"bytes\"},{\"name\":\"TOTALCOUNT\",\"type\":[\"null\",\"int\"]},{\"name\":\"UNITPRICE\",\"type\":[\"null\",\"bytes\"]}]}");

		public virtual Schema Schema
		{
			get
			{
				return KsqlDataSourceSchema._SCHEMA;
			}
		}
		public int TOTALCOUNT { get; set; }
		public decimal TOTALMONEY { get; set; }

		public byte[] UNITPRICE { get; set; }

		public object Get(int fieldPos)
		{
			switch (fieldPos)
			{
				case 0: return this.TOTALMONEY;
				case 1: return this.TOTALCOUNT;
				case 2: return this.UNITPRICE;
				default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
			};
		}

		public void Put(int fieldPos, object fieldValue)
		{
			switch (fieldPos)
			{
				case 0:
					var newval = (byte[])fieldValue;
					var result = System.Text.Encoding.Default.GetChars(newval);
					

					string output = string.Join(" ", result.Select(c => (uint)c));
		

					this.TOTALMONEY = Convert.ToDecimal(output); break;
				case 1: this.TOTALCOUNT = (System.Int32)fieldValue; break;
				case 2:

					this.UNITPRICE = (byte[])fieldValue; break;

				default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
			};
		}

		public static double ByteArrayToDouble(byte[] src)
		{
			using (MemoryStream stream = new MemoryStream(src))
			{
				stream.Position = 0;
				// Create a BinaryReader to read the decimal from the stream
				using (BinaryReader reader = new BinaryReader(stream))
				{
					// Read and return the decimal from the
					// BinaryReader/MemoryStream
					var aa = reader.ReadInt32();
					var ss = reader.ReadDecimal();

					return 0.0;
				}
			}

		}
	}
}
