function out = json(kind, arg)

%   JSON -- Helper function to write / decode json text.

switch ( kind )
  case 'encode'
    out = json_encoder.encode( arg );
  case 'parse'
    out = json_decoder.parse( arg );
  otherwise
    error( 'Specify ''encode'' or ''parse''' );
end

end