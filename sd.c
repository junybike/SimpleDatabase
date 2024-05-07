#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

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
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
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
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct 
{
    StatementType type;
    Row row_to_insert;
} Statement;

typedef struct
{
    int file_desc;
    uint32_t file_length;
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct 
{
    Pager* pager;
    uint32_t num_rows;
} Table;

typedef struct 
{
    Table* table;
    uint32_t row_num;
    bool end_of_table; // Indicates a position one past the last element
} Cursor;

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

Pager* pager_open(const char* filename)
{
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1)
    {
        printf("Error: unable to open file");
        exit(EXIT_FAILURE);
    }
    
    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager* pager = malloc(sizeof(Pager));
    pager->file_desc = fd;
    pager->file_length = file_length;

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        pager->pages[i] = NULL;
    }
    return pager;
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size)
{
    if (pager->pages[page_num] == NULL)
    {
        printf("Error: Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }
    
    off_t offset = lseek(pager->file_desc, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1)
    {
        printf("Error: Seeking %d", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t byte_written = write(pager->file_desc, pager->pages[page_num], size);
    if (byte_written == -1)
    {
        printf("Error: Writing %d", errno);
        exit(EXIT_FAILURE);
    }
}

Table* db_open(const char* filename)
{
    Pager* pager = pager_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;

    Table* table = malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = num_rows;

    return table;
}

void db_close(Table *table)
{
    Pager* pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < num_full_pages; i++)
    {
        if (pager->pages[i] == NULL)
        {
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if (num_additional_rows > 0)
    {
        uint32_t page_num = num_full_pages;
        if (pager->pages[page_num] != NULL)
        {
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
    }

    int result = close(pager->file_desc);
    if (result == -1)
    {
        printf("Error: Fail to close db file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        void* page = pager->pages[i];
        if (page)
        {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
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
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    else
    {
        return META_COMMAND_FAIL;
    }
}

PrepareResult prepare_insert(InputBuffer* input_buf, Statement* statement)
{
    statement->type = STATEMENT_INSERT;

    char* keyword = strtok(input_buf->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL)
    {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if (id < 0)
    {
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE || strlen(email) > COLUMN_EMAIL_SIZE)
    {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buf, Statement* statement)
{
    if (strncmp(input_buf->buffer, "insert", 6) == 0)
    {
        return prepare_insert(input_buf, statement);
    }
    if (strcmp(input_buf->buffer, "select") == 0)
    {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_FAIL;
}

void* get_page(Pager* pager, uint32_t page_num)
{
    if (page_num > TABLE_MAX_PAGES)
    {
        printf("Error: Tried to fetch page number out of bounds. %d > %d",page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL)
    {
        // Cache miss. Allocate memory and load from file.
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        // Case where saving partial page at the end of file.
        if (pager->file_length % PAGE_SIZE)
        {
            num_pages++;
        }

        if (page_num <= num_pages)
        {
            lseek(pager->file_desc, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_desc, page, PAGE_SIZE);
            if (bytes_read == -1)
            {
                printf("Error: Fail to read file '%d'\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}

void serialize_row(Row* source, void* destination)
{
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    strncpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    strncpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination)
{
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void* cursor_value(Cursor* cursor)
{
    uint32_t row_num = cursor->row_num;
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    
    void* page = get_page(cursor->table->pager, page_num);
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
    serialize_row(row_to_insert, cursor_value(cursor));
    table->num_rows += 1;
    free(cursor);
    
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table)
{
    Cursor* cursor = table_start(table);
    Row row;
    while (!(cursor->end_of_table))
    {
        deserialize_row(cursor_value(cursor), &row);
        printf(&row);
        cursor_advance(cursor);
    }
    free(cursor);
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

Cursor* table_start(Table* table)
{
    Cursor* cursor = malloc(sizeof(cursor));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table = (table->num_rows == 0);

    return cursor;
}

Cursor* table_end(Table* table)
{
    Cursor* cursor = malloc(sizeof(cursor));
    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->end_of_table = true;

    return cursor;
}

void cursor_advance(Cursor* cursor)
{
    cursor->row_num += 1;
    if (cursor->row_num >= cursor->table->num_rows)
    {
        cursor->end_of_table = true;
    }
}

int main(int argc, char* argv[])
{
    if (argc < 2)
    {
        printf("./d <database filename>\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    Table* table = db_open(filename);

    InputBuffer* input_buf = new_input_buffer();

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

            case (PREPARE_NEGATIVE_ID):
                printf("ERROR: ID must be positive");
            
            case (PREPARE_STRING_TOO_LONG):
                printf("ERROR: String is too long");
                continue;

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
