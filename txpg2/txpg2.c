#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <assert.h>

#include "txpg2.h"


void x(int numCols, char *input) {
    fieldData_t *s[50];

    const char *current = input;
    int i = 0;
    while (numCols--) {
        s[i] = (fieldData_t*) current;
        s[i]->length = ntohl(s[i]->length);
        current += (s[i])->length + 4;
        i++;
    }
    return;
}


inline uint16_t READ_SHORT(const char **buf){
    uint16_t rv = ntohs(*(uint16_t*) (*buf));
    *buf += sizeof(uint16_t);
    return rv;
}

inline uint32_t READ_INT(const char **buf){
    uint32_t rv = ntohl(*(uint32_t*) (*buf));
    *buf += sizeof(uint32_t);
    return rv;
}


fieldDescr_t *parse_rowDescription(const char *input, size_t buflen){
    short num = READ_SHORT(&input);

    fieldDescr_t *descrs = malloc(sizeof(fieldDescr_t) * num);
    assert(buflen == 2 + sizeof(fieldDescr) * num);

    size_t fieldname_len;

    for (int i = 0; i < num; i++){
        fieldname_len = strlen(input);
        descrs[i].fieldname = malloc(fieldname_len + 1);
        strncpy(descrs[i].fieldname, input, fieldname_len + 1);

        input += strlen(input) + 1;
        descrs[i].table_oid = READ_INT(&input);
        descrs[i].attrnum = READ_SHORT(&input);
        descrs[i].type_oid = READ_INT(&input);
        printf("%s: %d\n", descrs[i].fieldname, descrs[i].type_oid);
        descrs[i].typlen = READ_SHORT(&input);
        descrs[i].typmod = READ_INT(&input);
        descrs[i].format_code = READ_SHORT(&input);
    }
    return descrs;
}

