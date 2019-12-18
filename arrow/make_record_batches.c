#include <stdlib.h>
#include <arrow-glib/arrow-glib.h>

const int num_of_records = 1000;

GList *build_array_and_append_to_list(const int array_length, GList *list_of_arrays)
{
    gboolean success = TRUE;
    GError *error = NULL;

    GArrowInt64ArrayBuilder *array_builder = garrow_int64_array_builder_new();

    int i = 0;
    while (success && i < array_length)
    {
        success = garrow_int64_array_builder_append_value(array_builder, i, &error);
        i++;
    }

    GArrowArray *col = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(array_builder), &error);
    list_of_arrays = g_list_append(list_of_arrays, col);
    return list_of_arrays;
}

int main(int argc, char **argv)
{
    GError *error = NULL;

    // Cast GArrowInt64DataType* to GArrowDataType* to make the compiler not complain
    GArrowDataType *garrow_int64 = (GArrowDataType *)garrow_int64_data_type_new();

    GList *fields = NULL;
    fields = g_list_append(fields, garrow_field_new("a", garrow_int64));
    fields = g_list_append(fields, garrow_field_new("b", garrow_int64));
    fields = g_list_append(fields, garrow_field_new("c", garrow_int64));
    fields = g_list_append(fields, garrow_field_new("d", garrow_int64));

    GArrowSchema *schema = garrow_schema_new(fields);

    gchar *schema_str = garrow_schema_to_string(schema);
    g_print("Schema: \n%s \n", schema_str);

    GList *list_of_columns = NULL;
    const int num_cols = 4;
    int i;
    for (i = 0; i < num_cols; i++)
    {
        list_of_columns = build_array_and_append_to_list(num_of_records, list_of_columns);
    }

    GArrowRecordBatch *record_batch = garrow_record_batch_new(schema, num_of_records, list_of_columns, &error);

    gchar *record_batch_str = garrow_record_batch_to_string(record_batch, &error);
    g_print("Record Batch: \n%s \n", record_batch_str);

    guint n_columns = garrow_record_batch_get_n_columns(record_batch);
    g_print("Number of columns in the Record Batch: %d \n", n_columns);

    gint64 n_rows = garrow_record_batch_get_n_rows(record_batch);
    g_print("Number of rows in the Record Batch: %lld \n", n_rows);
}
