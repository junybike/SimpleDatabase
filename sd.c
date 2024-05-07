#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

typedef struct 
{
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef enum
{
    META_COMMAND_SUCCESS,
    META_COMMAND_FAIL
} MetaCommandResult;

typedef enum
{
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_FAIL
} PrepareResult;

typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef enum
{
    EXECUTE_SUCCESS, 
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef struct 
{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct 
{
    StatementType type;
    Row row_to_insert;
} Statement;

typedef struct 
{
    uint32_t num_rows;
    void* pages[TABLE_MAX_PAGES];
} Table;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;

const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

InputBuffer* new_input_buffer()
{
    InputBuffer* input_buf = (InputBuffer*)malloc(sizeof(InputBuffer));
    input_buf->buffer = NULL;
    input_buf->buffer_length = 0;
    input_buf->input_length = 0;

    return input_buf;
}

Table* new_table()
{
    Table* table = (Table*)malloc(sizeof(Table));
    table->num_rows = 0;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        table->pages[i] = NULL;
    }
    return table;
}

void free_table(Table* table)
{
    for (int i = 0; table->pages[i]; i++)
    {
        free(table->pages[i]);
    }
    free(table);
}

void print_prompt()
{
    printf("db > ");
}

void print_row(Row* row)
{
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void read_input(InputBuffer* input_buf)
{
    ssize_t bytes_read = getline(&(input_buf->buffer), &input_buf->buffer_length, stdin);
    if (bytes_read <= 0)
    {
        printf("Error: Reading input failed");
        exit(EXIT_FAILURE);
    }
    input_buf->input_length = bytes_read - 1;
    input_buf->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer* input_buf)
{
    free(input_buf->buffer);
    free(input_buf);
}

MetaCommandResult do_meta_command(InputBuffer* input_buf, Table *table)
{
    if (strcmp(input_buf->buffer, ".exit") == 0)
    {
        close_input_buffer(input_buf);
        free_table(table);
        exit(EXIT_SUCCESS);
    }
    else
    {
        return META_COMMAND_FAIL;
    }
}

PrepareResult prepare_statement(InputBuffer* input_buf, Statement* statement)
{
    if (strncmp(input_buf->buffer, "insert", 6) == 0)
    {
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(input_buf->buffer, "insert %d %s %s", 
            &(statement->row_to_insert.id), statement->row_to_insert.username, statement->row_to_insert.email);

        if (args_assigned < 3)
        {
            printf("insert <id> <name> <email>\n");
            return PREPARE_SYNTAX_ERROR;
        }

        return PREPARE_SUCCESS;
    }
    if (strcmp(input_buf->buffer, "select") == 0)
    {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_FAIL;
}

void serialize_row(Row* source, void* destination)
{
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination)
{
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void* row_slot(Table* table, uint32_t row_num)
{
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = table->pages[page_num];
    
    if (page == NULL)
    {
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

ExecuteResult execute_insert(Statement* statement, Table* table)
{
    if (table->num_rows >= TABLE_MAX_ROWS)
    {
        return EXECUTE_TABLE_FULL;
    }

    Row* row_to_insert = &(statement->row_to_insert);
    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    table->num_rows += 1;
    
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table)
{
    Row row;
    for (uint32_t i = 0; i < table->num_rows; i++)
    {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table)
{
    switch (statement->type)
    {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
    }
    return EXECUTE_TABLE_FULL;
}

int main(int argc, char* argv[])
{
    InputBuffer* input_buf = new_input_buffer();
    Table* table = new_table();
    
    while(1)
    {
        print_prompt();
        read_input(input_buf);

        if (input_buf->buffer[0] == '.')
        {
            switch (do_meta_command(input_buf, table))
            {
                case (META_COMMAND_SUCCESS):
                    continue;
                    
                case (META_COMMAND_FAIL):
                    printf("Unrecognized command '%s'\n", input_buf->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buf, &statement))
        {
            case (PREPARE_SUCCESS):
                break;
            
            case (PREPARE_SYNTAX_ERROR):
                printf("Error: Syntax error\n");
                continue;

            case (PREPARE_FAIL):
                printf("Error: Unrecognized keyword at start of '%s'.\n", input_buf->buffer);
                continue;
        }

        switch (execute_statement(&statement, table))
        {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table is full\n");
                break;
        }
    }
}
