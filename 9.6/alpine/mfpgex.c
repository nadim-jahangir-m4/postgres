/*
Author: Nadim Jahangir

Compile using: # requires postgresql-devel package
gcc -fpic -c mfpgex.c -I/usr/include/pgsql/server/
gcc -shared -o mfpgex.so mfpgex.o
*/

#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define DELIMITER 0
#define NUM_OFFSET 999999999999999999LL

static const char *DIGIT = "0123456789ABCDEF";

#define HEX(a, i, v) a[i++] = DIGIT[(v >> 4) & 0xf]; a[i++] = DIGIT[v & 0xf]

/* http://stjarnhimlen.se/snippets/jdn.c */
static int julian(int y, int m, int d)
{
	return (int) (d - 32076)
		+ 1461L * (y + 4800 + (m - 14) / 12) / 4
		+ 367 * (m - 2 - (m - 14) / 12 * 12) / 12
		- 3 * ((y + 4900 + (m - 14) / 12) / 100) / 4
		+ 1;
}

/* couldn't find a better name! */
static long long longify(char *type, char *val)
{
    char *pdot = strchr(type, '.');
    char *p;
    char tmp1[50];
    char tmp2[50];
    int i;

    for (i = 0; val[i]; i++) tmp1[i] = val[i];

    if (pdot) /* has '.' */
    {
        tmp1[i++] = 'e';
        for (p = pdot + 1; *p && *p != '('; p++)
        {
            tmp1[i++] = *p;
        }
    }
    tmp1[i] = 0;
    sprintf(tmp2, "%.0lf", atof(tmp1));
    
    return strtoll(tmp2, NULL, 10) + NUM_OFFSET;
}

/*
CREATE FUNCTION norm_key_val(text[], text[]) RETURNS text AS '/usr/lib/mfpgex.so', 'norm_key_val' LANGUAGE C IMMUTABLE;
*/
PG_FUNCTION_INFO_V1(norm_key_val);
Datum norm_key_val(PG_FUNCTION_ARGS)
{
    ArrayType *types_arr, *vals_arr;
    Datum *types, *vals;
    Oid types_eltype, vals_eltype;
    int16 types_typlen, vals_typlen;
    bool types_typbyval, vals_typbyval;
    char types_typalign, vals_typalign;
    int ntypes, nvals;
    bool *types_nulls, *vals_nulls;

	char *ret;
	int ret_len;
	Datum retd;
	
	char *typea[10] = {0};
	char *vala[10] = {0};
	int val_len[10] = {0};
	
	char typ;
	
	int i, j, k, l;
	int all_empty;
	
	char c;
	unsigned char *byte_ptr;
	long long ll;
	unsigned char byte;
	int hour, min, sec;
	int yr, mn, dy, days;
	char tmp[50];
	
	types_arr = PG_GETARG_ARRAYTYPE_P(0);
    types_eltype = ARR_ELEMTYPE(types_arr);
    get_typlenbyvalalign(types_eltype, &types_typlen, &types_typbyval, &types_typalign);
    deconstruct_array(types_arr, types_eltype, types_typlen, types_typbyval, types_typalign, &types, &types_nulls, &ntypes);
    
    vals_arr = PG_GETARG_ARRAYTYPE_P(1);
    vals_eltype = ARR_ELEMTYPE(vals_arr);
    get_typlenbyvalalign(vals_eltype, &vals_typlen, &vals_typbyval, &vals_typalign);
    deconstruct_array(vals_arr, vals_eltype, vals_typlen, vals_typbyval, vals_typalign, &vals, &vals_nulls, &nvals);
    
    if (ntypes != nvals)
    {
    	pfree(types);
		pfree(vals);
		pfree(types_nulls);
		pfree(vals_nulls);
		
        elog(ERROR, "The number of values is not equal to the number of types");
    }
    
    ret_len = ntypes - 1;
    for (i = 0; i < ntypes; i++) 
    {
        if (types_nulls[i])
        {
        	pfree(types);
			pfree(vals);
			pfree(types_nulls);
			pfree(vals_nulls);
			
            elog(ERROR, "Type[%d] cannot be NULL", i + 1);
        }
        typea[i] = DatumGetCString(DirectFunctionCall1(textout, types[i]));
        
        if (vals_nulls[i]) 
        {
        	vala[i] = NULL;
        	val_len[i] = 0;
        }
        else
        {
        	vala[i] = DatumGetCString(DirectFunctionCall1(textout, vals[i]));
        	val_len[i] = strlen(vala[i]);
       	}
        
        typ = typea[i][0];
        
        switch (typ)
        {
        	case 'I':
        	case 'Z':
        	case 'M':
        	case 'N':
        		ret_len += 8;
        		break;
        	case 'D':
        	case 'T':
        		ret_len += 3;
        		break;
        	default:
        		ret_len += val_len[i];
        }
    }

	/* check if all fields (but bookmark) are empty */
	all_empty = 1;
	for (i = nvals - 2; i >= 0; i--)
	{
		if (val_len[i]) 
		{
			all_empty = 0;
			break;
		}
	}
	
	/* if all fields are empty then return null */
	if (all_empty)
	{
		for (i = 0; i < nvals; i++)
		{
			if (vala[i]) pfree(vala[i]);
	        pfree(typea[i]);
		}
		
		pfree(types);
		pfree(vals);
	   
		pfree(types_nulls);
		pfree(vals_nulls);
		
		PG_RETURN_NULL();
	}
	
	ret = (char *) malloc((ret_len << 1) | 1);
	k = 0;
    for (i = 0; i < nvals; i++) 
    {
    	if (i) { HEX(ret, k, DELIMITER); }
    	
		typ = typea[i][0];
    	
        if (typ == 'I' || typ == 'Z')
        {
        	ll = 0;
        	if (vala[i]) ll = NUM_OFFSET + strtoll(vala[i], NULL, 10);
        	for (j = 56; j >= 0; j -= 8) {
        		byte = (ll >> j) & 0xff;
        		HEX(ret, k, byte);
        	}
        }
        else if (typ == 'D')
        {
        	if (!vala[i] || val_len[i] != 10) /* if not in 'YYYY-MM-DD' format */
        	{
        		HEX(ret, k, 0);
        		HEX(ret, k, 0);
        		HEX(ret, k, 0);
        	}
        	else
        	{
        		yr = 1000 * (vala[i][0] - '0') + 100 * (vala[i][1] - '0') + 10 * (vala[i][2] - '0') + (vala[i][3] - '0');
        		mn = 10 * (vala[i][5] - '0') + (vala[i][6] - '0');
        		dy = 10 * (vala[i][8] - '0') + (vala[i][9] - '0');
        		
        		days = julian(yr, mn, dy);
        		
        		byte = (days >> 16) & 0xff;
        		HEX(ret, k, byte);
        		
        		byte = (days >> 8) & 0xff;
        		HEX(ret, k, byte);
        		
        		byte = days & 0xff;
        		HEX(ret, k, byte);
        	}
        }
        else if (typ == 'T')
        {	
        	if (!vala[i] || val_len[i] != 5) /* if not in 'HH:mm' format */
        	{
        		HEX(ret, k, 0);
        		HEX(ret, k, 0);
        		HEX(ret, k, 0);
        	}
        	else
        	{
        		hour = 10 * (vala[i][0] - '0') + (vala[i][1] - '0');
        		min = 10 * (vala[i][3] - '0') + (vala[i][4] - '0');
        		
        		sec = 3600 * hour + 60 * min;
        		
        		byte = (sec >> 16) & 0xff;
        		HEX(ret, k, byte);
        		
        		byte = (sec >> 8) & 0xff;
        		HEX(ret, k, byte);
        		
        		byte = sec & 0xff;
        		HEX(ret, k, byte);
        	}
        }
        else if (typ == 'N' || typ == 'M')
        {
        	ll = 0;
        	
        	if (val_len[i]) ll = longify(typea[i], vala[i]);
		    
		    for (j = 56; j >= 0; j -= 8) {
        		byte = (ll >> j) & 0xff;
        		HEX(ret, k, byte);
        	}
        }
        else
        {
        	if (val_len[i])
        	{
        		for (j = 0; j < val_len[i]; j++)
        		{
        			c = toupper(vala[i][j]);
        			HEX(ret, k, c);
        		}
        	}
        }
        
        if (vala[i]) pfree(vala[i]);
        pfree(typea[i]);
    }
    ret[k++] = 0;
    
    pfree(types);
    pfree(vals);
   
    pfree(types_nulls);
    pfree(vals_nulls);
    
    retd = DirectFunctionCall1(textin, CStringGetDatum(ret));
    
    free(ret);
    
    return retd;
}

