/* 
 * jsmn.h - minimalistic JSON parser in C
 * Source: https://github.com/zserge/jsmn (MIT License)
 * This file is included for convenience in this coursework project.
 *
 * NOTE: If you want to update/replace this file, keep the API compatible:
 *   - jsmn_init()
 *   - jsmn_parse()
 *   - jsmntok_t, jsmn_parser
 */

#ifndef JSMN_H
#define JSMN_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * JSON type identifier. Basic types are:
 *  o Object
 *  o Array
 *  o String
 *  o Other primitive: number, boolean (true/false) or null
 */
typedef enum {
    JSMN_UNDEFINED = 0,
    JSMN_OBJECT = 1,
    JSMN_ARRAY  = 2,
    JSMN_STRING = 3,
    JSMN_PRIMITIVE = 4
} jsmntype_t;

enum jsmnerr {
    /* Not enough tokens were provided */
    JSMN_ERROR_NOMEM = -1,
    /* Invalid character inside JSON string */
    JSMN_ERROR_INVAL = -2,
    /* The string is not a full JSON packet, more bytes expected */
    JSMN_ERROR_PART  = -3
};

/**
 * JSON token description.
 * type: type (object, array, string etc.)
 * start/end: token position in JSON data string
 * size: number of child tokens (for objects/arrays)
 */
typedef struct {
    jsmntype_t type;
    int start;
    int end;
    int size;
#ifdef JSMN_PARENT_LINKS
    int parent;
#endif
} jsmntok_t;

/**
 * JSON parser. Contains internal state.
 */
typedef struct {
    unsigned int pos;     /* offset in the JSON string */
    unsigned int toknext; /* next token to allocate */
    int toksuper;         /* superior token node, e.g parent object or array */
} jsmn_parser;

/**
 * Initialize a parser
 */
static void jsmn_init(jsmn_parser *parser) {
    parser->pos = 0;
    parser->toknext = 0;
    parser->toksuper = -1;
}

static jsmntok_t *jsmn_alloc_token(jsmn_parser *parser, jsmntok_t *tokens, size_t num_tokens) {
    if (parser->toknext >= num_tokens) return NULL;
    jsmntok_t *tok = &tokens[parser->toknext++];
    tok->start = tok->end = -1;
    tok->size = 0;
#ifdef JSMN_PARENT_LINKS
    tok->parent = -1;
#endif
    tok->type = JSMN_UNDEFINED;
    return tok;
}

static void jsmn_fill_token(jsmntok_t *token, jsmntype_t type, int start, int end) {
    token->type = type;
    token->start = start;
    token->end = end;
    token->size = 0;
}

static int jsmn_parse_primitive(jsmn_parser *parser, const char *js, size_t len,
                               jsmntok_t *tokens, size_t num_tokens) {
    int start = (int)parser->pos;

    for (; parser->pos < len; parser->pos++) {
        char c = js[parser->pos];
        switch (c) {
            case '\t': case '\r': case '\n': case ' ':
            case ',': case ']': case '}':
                goto found;
            default:
                /* valid primitive chars */
                if ((unsigned char)c < 32) return JSMN_ERROR_INVAL;
                break;
        }
    }
found:
    if (tokens == NULL) {
        parser->pos--;
        return 0;
    }
    jsmntok_t *token = jsmn_alloc_token(parser, tokens, num_tokens);
    if (token == NULL) {
        parser->pos = (unsigned int)start;
        return JSMN_ERROR_NOMEM;
    }
    jsmn_fill_token(token, JSMN_PRIMITIVE, start, (int)parser->pos);
#ifdef JSMN_PARENT_LINKS
    token->parent = parser->toksuper;
#endif
    parser->pos--;
    return 0;
}

static int jsmn_parse_string(jsmn_parser *parser, const char *js, size_t len,
                            jsmntok_t *tokens, size_t num_tokens) {
    int start = (int)parser->pos;
    parser->pos++;

    for (; parser->pos < len; parser->pos++) {
        char c = js[parser->pos];

        if (c == '\"') {
            if (tokens == NULL) return 0;
            jsmntok_t *token = jsmn_alloc_token(parser, tokens, num_tokens);
            if (token == NULL) {
                parser->pos = (unsigned int)start;
                return JSMN_ERROR_NOMEM;
            }
            jsmn_fill_token(token, JSMN_STRING, start + 1, (int)parser->pos);
#ifdef JSMN_PARENT_LINKS
            token->parent = parser->toksuper;
#endif
            return 0;
        }

        if (c == '\\' && parser->pos + 1 < len) {
            parser->pos++;
            c = js[parser->pos];
            /* Valid escaped chars per JSON spec */
            switch (c) {
                case '\"': case '/': case '\\': case 'b':
                case 'f': case 'r': case 'n': case 't':
                    break;
                case 'u':
                    /* Skip \uXXXX */
                    for (int i = 0; i < 4 && parser->pos + 1 < len; i++) {
                        char h = js[++parser->pos];
                        if (!((h >= '0' && h <= '9') || (h >= 'A' && h <= 'F') || (h >= 'a' && h <= 'f'))) {
                            return JSMN_ERROR_INVAL;
                        }
                    }
                    break;
                default:
                    return JSMN_ERROR_INVAL;
            }
        }
    }
    return JSMN_ERROR_PART;
}

/**
 * Parse JSON string and fill tokens.
 * @return number of tokens used, or negative error code.
 */
static int jsmn_parse(jsmn_parser *parser, const char *js, size_t len,
                      jsmntok_t *tokens, unsigned int num_tokens) {
    int r;
    int count = (int)parser->toknext;

    for (; parser->pos < len; parser->pos++) {
        char c = js[parser->pos];

        switch (c) {
            case '{':
            case '[': {
                count++;
                if (tokens == NULL) break;
                jsmntok_t *token = jsmn_alloc_token(parser, tokens, num_tokens);
                if (token == NULL) return JSMN_ERROR_NOMEM;
                if (parser->toksuper != -1) {
                    tokens[parser->toksuper].size++;
#ifdef JSMN_PARENT_LINKS
                    token->parent = parser->toksuper;
#endif
                }
                token->type = (c == '{' ? JSMN_OBJECT : JSMN_ARRAY);
                token->start = (int)parser->pos;
                parser->toksuper = (int)parser->toknext - 1;
                break;
            }
            case '}':
            case ']': {
                if (tokens == NULL) break;
                jsmntype_t type = (c == '}' ? JSMN_OBJECT : JSMN_ARRAY);
                int i;
                for (i = (int)parser->toknext - 1; i >= 0; i--) {
                    jsmntok_t *t = &tokens[i];
                    if (t->start != -1 && t->end == -1) {
                        if (t->type != type) return JSMN_ERROR_INVAL;
                        t->end = (int)parser->pos + 1;
                        parser->toksuper =
#ifdef JSMN_PARENT_LINKS
                            t->parent;
#else
                            -1;
                        /* Find the parent with an open end */
                        for (int j = i - 1; j >= 0; j--) {
                            if (tokens[j].start != -1 && tokens[j].end == -1) {
                                parser->toksuper = j;
                                break;
                            }
                        }
#endif
                        break;
                    }
                }
                if (i == -1) return JSMN_ERROR_INVAL;
                break;
            }
            case '\"':
                r = jsmn_parse_string(parser, js, len, tokens, num_tokens);
                if (r < 0) return r;
                count++;
                if (parser->toksuper != -1 && tokens != NULL) tokens[parser->toksuper].size++;
                break;
            case '\t': case '\r': case '\n': case ' ':
                break;
            case ':':
                parser->toksuper = (int)parser->toknext - 1;
                break;
            case ',':
                if (tokens != NULL && parser->toksuper != -1 &&
                    tokens[parser->toksuper].type != JSMN_ARRAY &&
                    tokens[parser->toksuper].type != JSMN_OBJECT) {
#ifdef JSMN_PARENT_LINKS
                    parser->toksuper = tokens[parser->toksuper].parent;
#else
                    parser->toksuper = -1;
                    for (int j = (int)parser->toknext - 1; j >= 0; j--) {
                        if (tokens[j].type == JSMN_ARRAY || tokens[j].type == JSMN_OBJECT) {
                            if (tokens[j].start != -1 && tokens[j].end == -1) {
                                parser->toksuper = j;
                                break;
                            }
                        }
                    }
#endif
                }
                break;
            default:
                r = jsmn_parse_primitive(parser, js, len, tokens, num_tokens);
                if (r < 0) return r;
                count++;
                if (parser->toksuper != -1 && tokens != NULL) tokens[parser->toksuper].size++;
                break;
        }
    }

    if (tokens != NULL) {
        for (int i = (int)parser->toknext - 1; i >= 0; i--) {
            if (tokens[i].start != -1 && tokens[i].end == -1) return JSMN_ERROR_PART;
        }
    }

    return count;
}

#ifdef __cplusplus
}
#endif

#endif /* JSMN_H */
