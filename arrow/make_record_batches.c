#include <stdlib.h>
#include <arrow-glib/arrow-glib.h>

int main(int argc, char **argv)
{
    GError *error = NULL;

    // Cast GArrowInt64DataType* to GArrowDataType* to make the compiler not complain
    GArrowDataType *garrow_int64 = (GArrowDataType *)garrow_int64_data_type_new();

    GArrowField *field_a = garrow_field_new("a", garrow_int64);
    GArrowField *field_b = garrow_field_new("b", garrow_int64);
    GArrowField *field_c = garrow_field_new("c", garrow_int64);
    GArrowField *field_d = garrow_field_new("d", garrow_int64);

    GList *fields = NULL;
    fields = g_list_append(fields, field_a);
    fields = g_list_append(fields, field_b);
    fields = g_list_append(fields, field_c);
    fields = g_list_append(fields, field_d);

    GArrowSchema *schema = garrow_schema_new(fields);

    gchar *schema_str = garrow_schema_to_string(schema);
    g_print("Schema: \n%s \n", schema_str);

    guint n_fields = garrow_schema_n_fields(schema);
    g_print("Number of fields in the schema: %d \n", n_fields);

    GArrowRecordBatchBuilder *record_batch_builder = garrow_record_batch_builder_new(schema, &error);

    GArrowRecordBatch *record_batch = garrow_record_batch_builder_flush(record_batch_builder, &error);

    gchar *record_batch_str = garrow_record_batch_to_string(record_batch, &error);
    g_print("Record Batch: \n%s \n", record_batch_str);

    guint n_columns = garrow_record_batch_get_n_columns(record_batch);
    g_print("Number of columns in the Record Batch: %d \n", n_columns);

    gint64 n_rows = garrow_record_batch_get_n_rows(record_batch);
    g_print("Number of rows in the Record Batch: %lld \n", n_rows);
}
