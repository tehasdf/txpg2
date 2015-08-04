typedef struct fieldDescr {
    char * fieldname;
    uint32_t table_oid;
    uint16_t attrnum;
    uint32_t type_oid;
    uint16_t typlen;
    uint32_t typmod;
    uint16_t format_code;
} fieldDescr_t;

typedef struct fieldData {
    union {
        char length_data[4];
        unsigned int length;
    };
    char value[];
} fieldData_t;

void x(int numCols, char *input);
fieldDescr_t *parse_rowDescription(const char *input, size_t buflen);
