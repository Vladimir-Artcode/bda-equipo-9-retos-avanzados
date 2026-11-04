import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TiendaJoin {

  // 1) LLAVE COMPUESTA: (id_cliente, etiqueta de orden)
  //    etiqueta "0" = dato del cliente, "1-fecha" = un pedido
  public static class LlaveCompuesta implements WritableComparable<LlaveCompuesta> {
    private int id;
    private String orden = "";
    public LlaveCompuesta() {}
    public LlaveCompuesta(int id, String orden) { this.id = id; this.orden = orden; }
    public int getId() { return id; }
    public String getOrden() { return orden; }
    public void write(DataOutput out) throws IOException { out.writeInt(id); out.writeUTF(orden); }
    public void readFields(DataInput in) throws IOException { id = in.readInt(); orden = in.readUTF(); }
    public int compareTo(LlaveCompuesta o) {      // SECONDARY SORT: primero id, luego orden
      int c = Integer.compare(id, o.id);
      return c != 0 ? c : orden.compareTo(o.orden);
    }
    @Override public int hashCode() { return id; }
    @Override public boolean equals(Object x) {
      return x instanceof LlaveCompuesta && compareTo((LlaveCompuesta) x) == 0;
    }
  }

  // 2) MAPPER: distingue cliente (3 campos) de pedido (4 campos)
  public static class MapperTienda extends Mapper<LongWritable, Text, LlaveCompuesta, Text> {
    @Override
    protected void map(LongWritable k, Text value, Context ctx) throws IOException, InterruptedException {
      String[] f = value.toString().trim().split(",");
      if (f.length == 3) {            // cliente: id,nombre,ciudad
        ctx.write(new LlaveCompuesta(Integer.parseInt(f[0]), "0"), new Text(f[1] + "," + f[2]));
      } else if (f.length == 4) {     // pedido: id_pedido,id_cliente,fecha,monto
        ctx.write(new LlaveCompuesta(Integer.parseInt(f[1]), "1-" + f[2]), new Text(f[0] + "," + f[3]));
      }
    }
  }

  // 3) PARTITIONER: reparte SOLO por id_cliente
  public static class PorCliente extends Partitioner<LlaveCompuesta, Text> {
    @Override
    public int getPartition(LlaveCompuesta k, Text v, int n) {
      return (k.getId() & Integer.MAX_VALUE) % n;
    }
  }

  // 4) COMPARADOR DE AGRUPACION: un reduce() por cada cliente
  public static class AgrupaPorCliente extends WritableComparator {
    public AgrupaPorCliente() { super(LlaveCompuesta.class, true); }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      return Integer.compare(((LlaveCompuesta) a).getId(), ((LlaveCompuesta) b).getId());
    }
  }

  // 5) REDUCER: el cliente llega primero (JOIN), luego pedidos por fecha
  public static class ReducerTienda extends Reducer<LlaveCompuesta, Text, Text, Text> {
    @Override
    protected void reduce(LlaveCompuesta key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
      int id = key.getId();
      String nombre = "";
      int total = 0;
      for (Text v : values) {            // al iterar, "key" se actualiza con cada valor
        String tag = key.getOrden();
        if (tag.equals("0")) {
          nombre = v.toString().split(",")[0];
        } else {
          String[] p = v.toString().split(",");
          total += Integer.parseInt(p[1]);
          ctx.write(new Text(id + " " + nombre),
                    new Text(tag.substring(2) + "\tpedido " + p[0] + "\t" + p[1]));
        }
      }
      ctx.write(new Text(id + " " + nombre), new Text("TOTAL\t" + total));
    }
  }

  public static void main(String[] args) throws Exception {
    Job job = Job.getInstance(new Configuration(), "tienda join + secondary sort");
    job.setJarByClass(TiendaJoin.class);
    job.setMapperClass(MapperTienda.class);
    job.setPartitionerClass(PorCliente.class);
    job.setGroupingComparatorClass(AgrupaPorCliente.class);
    job.setReducerClass(ReducerTienda.class);
    job.setMapOutputKeyClass(LlaveCompuesta.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
