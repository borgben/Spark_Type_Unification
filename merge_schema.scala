import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class MyCustomException(s: String) extends Exception(s) {} 

    def insert(list: List[StructField], i: Int, value: StructField): List[StructField] = list match {
    case head :: tail if i > 0 => head :: insert(tail, i-1, value)
    case _ => value :: list
    }

     def get_type_difference (type_pair:(DataType,DataType)):List[StructField] = 
     {
       type_pair match 
       {
         case (ArrayType(subtype_a,contains_null_a), ArrayType(subtype_b, contains_null_b)) => get_type_difference(subtype_a,subtype_b)
         case (StructType(struct_fields_a),StructType(struct_fields_b)) => struct_fields_a.diff(struct_fields_b).toList
         case _ => Nil
       }
     }

     def unify_spark_types (type_pair:(DataType,DataType)):DataType = 
     {
       type_pair match 
       {
         case (StringType, StringType) => StringType
         case (IntegerType, IntegerType) => IntegerType 
         case (DoubleType, DoubleType) => DoubleType
         case (BooleanType, BooleanType) => BooleanType
         case (TimestampType, TimestampType) => TimestampType
         case (ArrayType(subtype_a,contains_null_a), ArrayType(subtype_b, contains_null_b)) => ArrayType(unify_spark_types((subtype_a,subtype_b)), contains_null_a && contains_null_b)
         case (StructType(struct_fields_a),StructType(struct_fields_b)) => StructType(struct_fields_a.union(struct_fields_b).distinct)
         case _ => ShortType
       }
     }

    def merge_struct_fields(column_name:Char, struct_field_a:Seq[StructField], struct_field_b:Seq[StructField]):(String, String)= 
    {
        var output_string = ""
        var output_string_ = ""
        if (struct_field_a.size == struct_field_b.size)
        {
            for (x <- (struct_field_a zip struct_field_b))
            {
                x match
                {
                    case (StructField(name_a, _, _, _), _) => 
                    {
                        output_string = output_string + "'" + name_a + "' ," + column_name + "." + name_a
                        output_string_ = output_string_ + "'" + name_a + "' ," +column_name + "." + name_a
                    }
                } 
            }
            return (output_string, output_string_)
        }else{
            var struct_field_a_list = struct_field_a.toList
            var struct_field_b_list = struct_field_b.toList
            
            var range = 0 
            if (struct_field_a_list.size >= struct_field_b_list.size)
            {
                range = struct_field_a_list.size
            }else{
                range = struct_field_b_list.size
            }

            var x = 0
            while (x < range)
            {
                try{
                    struct_field_a_list(x)
                }catch{
                    case _: Throwable =>
                    {   
                        struct_field_b_list(x) match
                        {
                            case StructField(name_b, _, _, _) => 
                            {
                                output_string = output_string + "'" + name_b + "' ," +"null"
                                output_string_ = output_string_ + "'" + name_b + "' ," + column_name +"."+name_b
                                if (x == range-1)
                                {
                                    return (output_string,output_string_)
                                }
                            }
                        }
                    }
                }
                try{
                    struct_field_b_list(x)
                }catch{
                    case _: Throwable =>
                    {   
                        struct_field_a_list(x) match
                        {
                            case StructField(name_a, _, _, _) => 
                            {
                                output_string = output_string + "'" + name_a + "' ," +"null"
                                output_string_ = output_string_ + "'" + name_a + "' ," + column_name +"."+name_a
                                if (x == range-1)
                                {
                                    return (output_string,output_string_)
                                }
                            }
                        }
                    }
                }
               (struct_field_a_list(x),struct_field_b_list(x)) match 
               {
                    case (StructField(name_a, data_type_a, is_null_a, meta_a), StructField(name_b,data_type_b,is_null_b,meta_b)) => 
                    {
                        if (name_a == name_b)
                        {
                            output_string = output_string + "'" + name_a + "' ," +column_name + "." + name_a
                            output_string_ = output_string_ + "'" + name_a + "' ," +column_name + "." + name_a
                        }
                        else
                        {
                            if(struct_field_a_list.size == range)
                            {
                                struct_field_b_list = insert(struct_field_b_list,x,StructField(name_a,data_type_a,is_null_a, meta_a))
                                output_string = output_string + "'" + name_a + "' ," + "null"
                                output_string_ = output_string_ + "'" + name_a + "' ," + column_name +"."+name_a
                            }
                            else
                            {
                                struct_field_b_list = insert(struct_field_a_list,x,StructField(name_b,data_type_b,is_null_b, meta_b))
                                output_string = output_string + "'" + name_b + "' ," +  "null"
                                output_string_ = output_string_ + "'" + name_b + "' ," + column_name +"."+name_b
                            }
                        }

                        if (x < (range-1))
                        {
                            output_string = output_string + ", "
                            output_string_ = output_string_ + ", "
                        }
                        x = x+1
                    }
               }
            }
            return (output_string, output_string_)
        }
    }

    def merge_complex_spark_types (column_name:String, anon_var:Int = 97,type_pair:(DataType,DataType)):(String,String) = 
     {
       type_pair match 
       {
         case (ArrayType(subtype_a,_), ArrayType(subtype_b, _)) => merge_complex_spark_types(column_name, anon_var,(subtype_a,subtype_b))
         case (StructType(struct_fields_a),StructType(struct_fields_b)) => 
         {
            merge_struct_fields(anon_var.toChar,struct_fields_a, struct_fields_b) match
             {
                case (output_string, output_string_) => 
                {
                    (
                        "transform(" + column_name + ","+anon_var.toChar+" -> named_struct("+output_string+"))",
                        "transform(" + column_name + ","+anon_var.toChar+" -> named_struct("+ output_string_ +"))"
                    )
                }
             }   
            
         }
         case _ => ("","")
       }
     }

    def merge_spark_df(column_diff:List[(String, DataType, DataType)]):(String,String) = 
    {
        for (x <- column_diff)
        {
            x match 
            {
                case (column_name, type_a, type_b) => 
                {
                    return merge_complex_spark_types(column_name, 97,(type_a, type_b))
                }
            }
        }
        return ("","")
    }

     // We assume the lists are the same size 
     def unify_spark_df(zipped_type_list:List[((String, DataType),(String, DataType))]): List[(String, DataType, DataType)]=
     {
       zipped_type_list match 
       {
         case ((name_a,type_a),(name_b,type_b))::tl =>
         {
           if (name_a == name_b) {
             val unified_type = unify_spark_types((type_a,type_b))
             if (unified_type == type_a)
             {
               unify_spark_df(tl)
             }else
             {
               (name_a,type_a,unified_type)::unify_spark_df(tl)
             }
           }
           else
           {
             throw new MyCustomException("Exception caught in unify_spark_df: " + name_a +" doesn't match "+ name_b)
           }
         }
         case Nil => 
         {
           List[(String, DataType, DataType)]()
         }
       }
     }

  val structureData = Seq(
    Row(Seq(Row("James ","Smith")),"36636","M",3100),
    Row(Seq(Row("Michael ","Rose","")),"40288","M",4300),
    Row(Seq(Row("Robert ","Williams")),"42114","M",1400)
  )

  val structureData2 = Seq(
        Row(Seq(Row("Maria ","Anne","Jones")),"39192","F",5500),
        Row(Seq(Row("Jen","Mary","Brown")),"","F",-1)
  )

  val structureSchema = new StructType()
    .add("name",new ArrayType(new StructType().add("firstname",StringType).add("lastname",StringType),false))
    .add("id",StringType)
    .add("gender",StringType)
    .add("salary",IntegerType)

  val structureSchema2 = new StructType()
    .add("name",new ArrayType(new StructType().add("firstname",StringType).add("middlename",StringType).add("lastname",StringType),false))
    .add("id",StringType)
    .add("gender",StringType)
    .add("salary",IntegerType)

  val df = spark.createDataFrame(
     spark.sparkContext.parallelize(structureData),structureSchema)
  df.printSchema()
  df.show()

    val df2 = spark.createDataFrame(
     spark.sparkContext.parallelize(structureData2),structureSchema2)
  df2.printSchema()
  df2.show()

  val df_datatypes_cols = df.columns.map(str => col(str))
  val df2_datatypes_cols = df2.columns.map(str => col(str))

  val df_datatypes  = df.select(df_datatypes_cols:_*).schema.fields.map(f => (f.name,f.dataType)).toList
  val df2_datatypes = df2.select(df2_datatypes_cols:_*).schema.fields.map(f => (f.name,f.dataType)).toList



merge_spark_df(unify_spark_df(df_datatypes zip df2_datatypes)) match 
{
    case (output_string,output_string_) => 
    {
          val df3 = df.withColumn("name",expr(output_string)).union(df2.withColumn("name",expr(output_string_)))

  df3.printSchema()
  df3.show()
    }
}
