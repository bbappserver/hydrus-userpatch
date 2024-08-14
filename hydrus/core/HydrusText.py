import typing

try:
    
    import chardet
    
    CHARDET_OK = True
    
except:
    
    CHARDET_OK = False
    

import json
import re

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusNumbers

re_one_or_more_whitespace = re.compile( r'\s+' ) # this does \t and friends too
# want to keep the 'leading space' part here, despite tag.strip() elsewhere, in case of some crazy '- test' tag
re_leading_garbage = re.compile( r'^(-|system:)+' )
re_leading_single_colon = re.compile( '^:(?!:)' )
re_leading_single_colon_and_no_more_colons = re.compile( '^:(?=[^:]+$)' )
re_leading_single_colon_and_later_colon = re.compile( '^:(?=[^:]+:[^:]+$)' )
re_leading_double_colon = re.compile( '^::(?!:)' )
re_leading_colons = re.compile( '^:+' )
re_leading_byte_order_mark = re.compile( '^' + HC.UNICODE_BYTE_ORDER_MARK ) # unicode .txt files prepend with this, wew

HYDRUS_NOTE_NEWLINE = '\n'

def CleanNoteText( t: str ):
    
    # trim leading and trailing whitespace
    
    t = t.strip()
    
    # wash all newlines
    
    lines = t.splitlines()
    
    # now trim each line
    
    lines = [ line.strip() for line in lines ]
    
    t = HYDRUS_NOTE_NEWLINE.join( lines )
    
    # now replace big gaps with reasonable ones
    
    double_newline = HYDRUS_NOTE_NEWLINE * 2
    triple_newline = HYDRUS_NOTE_NEWLINE * 3
    
    while triple_newline in t:
        
        t = t.replace( triple_newline, double_newline )
        
    
    return t
    

def ConvertManyStringsToNiceInsertableHumanSummary( texts: typing.Collection[ str ], do_sort: bool = True, no_trailing_whitespace = False ) -> str:
    """
    The purpose of this guy is to convert your list of 20 subscription names or whatever to something you can present to the user without making a giganto tall dialog.
    """
    texts = list( texts )
    
    if do_sort:
        
        SortStringsIgnoringCase( texts )
        
    
    if len( texts ) == 1:
        
        if no_trailing_whitespace:
            
            return f' "{texts[0]}"'
            
        else:
            
            return f' "{texts[0]}" '
            
        
    else:
        
        if len( texts ) <= 4:
            
            t = '\n'.join( texts )
            
        else:
            
            t = ', '.join( texts )
            
        
        if no_trailing_whitespace:
            
            return f'\n\n{t}'
            
        else:
            
            return f'\n\n{t}\n\n'
            
        
    

def ConvertManyStringsToNiceInsertableHumanSummarySingleLine( texts: typing.Collection[ str ], collective_description_noun: str, do_sort: bool = True ) -> str:
    """
    The purpose of this guy is to convert your list of 20 subscription names or whatever to something you can present to the user without making a giganto tall dialog.
    Suitable for a menu!
    """
    if len( texts ) == 0:
        
        return f'0(?) {collective_description_noun}'
        
    
    texts = list( texts )
    
    if do_sort:
        
        SortStringsIgnoringCase( texts )
        
    
    LINE_NO_LONGER_THAN = 48
    
    if len( texts ) == 1:
        
        text = texts[0]
        
        if len( text ) + 2 > LINE_NO_LONGER_THAN:
            
            return f'1 {collective_description_noun}'
            
        else:
            
            return f'"{text}"'
            
        
    else:
        
        if sum( ( len( text ) + 4 for text in texts ) ) > LINE_NO_LONGER_THAN:
            
            first_text = texts[0]
            
            possible = f'"{first_text}" & {HydrusNumbers.ToHumanInt(len(texts)-1)} other {collective_description_noun}'
            
            if len( possible ) <= LINE_NO_LONGER_THAN:
                
                return possible
                
            else:
                
                return f'{HydrusNumbers.ToHumanInt(len(texts))} {collective_description_noun}'
                
            
        else:
            
            return ', '.join( ( f'"{text}"' for text in texts ) )
            
        
    

def HexFilter( text ):
    
    text = text.lower()
    
    text = re.sub( '[^0123456789abcdef]', '', text )
    
    return text
    
def DeserialiseNewlinedTexts( text ):
    
    texts = text.splitlines()
    
    texts = [ StripIOInputLine( line ) for line in texts ]
    
    texts = [ line for line in texts if line != '' ]
    
    return texts
    
def ElideText( text, max_length, elide_center = False ):
    
    if len( text ) > max_length:
        
        if elide_center:
            
            CENTER_END_CHARS = max( 2, max_length // 8 )
            
            text = '{}{}{}'.format( text[ : max_length - ( 1 + CENTER_END_CHARS ) ], HC.UNICODE_ELLIPSIS, text[ - CENTER_END_CHARS : ] )
            
        else:
            
            text = '{}{}'.format( text[ : max_length - 1 ], HC.UNICODE_ELLIPSIS )
            
        
    
    return text
    

def GetFirstLine( text: str ) -> str:
    
    if len( text ) > 0:
        
        return text.splitlines()[0]
        
    else:
        
        return ''
        
    

def LooksLikeHTML( file_data: typing.Union[ str, bytes ] ):
    # this will false-positive if it is json that contains html, ha ha
    
    if isinstance( file_data, bytes ):
        
        search_elements = ( b'<html', b'<HTML', b'<!DOCTYPE html', b'<!DOCTYPE HTML' )
        
    else:
        
        search_elements = ( '<html', '<HTML', '<!DOCTYPE html', '<!DOCTYPE HTML' )
        
    
    for s_e in search_elements:
        
        if s_e in file_data:
            
            return True
            
        
    
    return False

def LooksLikeSVG( file_data ):
    
    if isinstance( file_data, bytes ):
        
        search_elements = ( b'<svg', b'<SVG', b'<!DOCTYPE svg', b'<!DOCTYPE SVG' )
        
    else:
        
        search_elements = ( '<svg', '<SVG', '<!DOCTYPE svg', '<!DOCTYPE SVG' )
        
    
    for s_e in search_elements:
        
        if s_e in file_data:
            
            return True
            
        
    
    return False
    

def LooksLikeJSON( file_data: typing.Union[ str, bytes ] ) -> bool:
    
    try:
        
        if isinstance( file_data, bytes ):
            
            file_data = str( file_data, 'utf-8' )
            
        
        json.loads( file_data )
        
        return True
        
    except:
        
        return False
        
    

NULL_CHARACTER = '\x00'

def ChardetDecode( data ):
    
    chardet_result = chardet.detect( data )
    
    chardet_confidence = chardet_result[ 'confidence' ]
    
    chardet_encoding = chardet_result[ 'encoding' ]
    
    chardet_text = str( data, chardet_encoding, errors = 'replace' )
    
    chardet_error_count = chardet_text.count( HC.UNICODE_REPLACEMENT_CHARACTER )
    
    return ( chardet_text, chardet_encoding, chardet_confidence, chardet_error_count )

def DefaultDecode( data ):
    
    default_encoding = 'windows-1252'
    
    default_text = str( data, default_encoding, errors = 'replace' )
    
    default_error_count = default_text.count( HC.UNICODE_REPLACEMENT_CHARACTER )
    
    return ( default_text, default_encoding, default_error_count )
    

# ISO is the official default I understand, absent an explicit declaration in http header or html document
# win-1252 is often assigned as an unofficial default after a scan suggests more complicated characters than the ISO
# I believe I have seen requests give both as default, but I am only super confident in the former
DEFAULT_WEB_ENCODINGS = ( 'ISO-8859-1', 'Windows-1252' )

def NonFailingUnicodeDecode( data, encoding, trust_the_encoding = False ):
    
    if trust_the_encoding:
        
        try:
            
            text = str( data, encoding, errors = 'replace' )
            
            return ( text, encoding )
            
        except:
            
            # ok, the encoding type wasn't recognised locally or something, so revert to trying our best
            encoding = None
            trust_the_encoding = False
            
        
    
    text = None
    confidence = None
    error_count = None
    
    try:
        
        ruh_roh_a = CHARDET_OK and encoding in DEFAULT_WEB_ENCODINGS
        ruh_roh_b = encoding is None
        
        if ruh_roh_a or ruh_roh_b:
            
            # ok, the site delivered one of these 'default' encodings. this is probably actually requests filling this in as default
            # we don't want to trust these because they are very permissive sets and'll usually decode garbage without errors
            # we want chardet to have a proper look and then compare them
            
            raise LookupError()
            
        
        text = str( data, encoding )
        
    except ( UnicodeDecodeError, LookupError ) as e:
        
        try:
            
            if encoding is not None:
                
                text = str( data, encoding, errors = 'replace' )
                
                confidence = 0.7
                error_count = text.count( HC.UNICODE_REPLACEMENT_CHARACTER )
                
            
            if CHARDET_OK:
                
                ( chardet_text, chardet_encoding, chardet_confidence, chardet_error_count ) = ChardetDecode( data )
                
                if chardet_error_count == 0:
                    
                    chardet_is_better = True
                    
                else:
                    
                    chardet_confidence_is_better = confidence is None or chardet_confidence > confidence
                    chardet_errors_is_as_good_or_better = error_count is None or chardet_error_count <= error_count
                    
                    chardet_is_better = chardet_confidence_is_better and chardet_errors_is_as_good_or_better
                    
                
                if chardet_is_better:
                    
                    text = chardet_text
                    encoding = chardet_encoding
                    
                
            
            if text is None:
                
                try:
                    
                    ( default_text, default_encoding, default_error_count ) = DefaultDecode( data )
                    
                    text = default_text
                    encoding = default_encoding
                    
                except:
                    
                    text = f'Could not decode the page--problem with given encoding "{encoding}" and no chardet library available.'
                    encoding = 'utf-8'
                    
                
            
            if text is None:
                
                raise Exception()
                
            
        except Exception as e:
            
            text = f'Unfortunately, could not decode the page with given encoding "{encoding}".'
            encoding = 'utf-8'
            
        
    
    if NULL_CHARACTER in text:
        
        # I guess this is valid in unicode for some reason
        # funnily enough, it is not replaced by 'replace'
        # nor does it raise an error in normal str creation
        
        text = text.replace( NULL_CHARACTER, '' )
        
    
    return ( text, encoding )
    

def RemoveNewlines( text: str ) -> str:
    
    good_lines = [ l.strip() for l in text.splitlines() ]
    
    good_lines = [ l for l in good_lines if l != '' ]
    
    # I really want to make this ' '.join(), but I'm sure that would break some old parsers
    text = ''.join( good_lines )
    
    return text
    

def SortStringsIgnoringCase( list_of_strings: typing.List[ str ] ):
    
    list_of_strings.sort( key = lambda s: s.lower() )
    

def StripIOInputLine( t ):
    
    t = re_leading_byte_order_mark.sub( '', t )
    
    t = t.strip()
    
    return t
    
