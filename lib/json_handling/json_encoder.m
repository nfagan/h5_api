classdef json_encoder < handle
  
  properties (Access = private)
    encoded;
  end
  
  methods (Access = private)
    
    function obj = json_encoder()
      obj.encoded = '';
    end
    
    function obj = encode_next(obj, value)
      
      switch ( class(value) )
        case 'cell'
          obj.encode_cell( value );
        case 'struct'
          obj.encode_struct( value );
        otherwise
          if ( ischar(value) || isscalar(value) )
            obj.encode_char_or_number( value );
          else
            assert( size(value, 1) <= 1, ['Cannot currently encode non-row' ...
              , ' vectors.'] );
            obj.encode_double_array( value );
          end
      end
    end
    
    function obj = encode_char_or_number(obj, value)
      
      if ( ~isa(value, 'char') )
        value = num2str( value );
      else value = sprintf( '"%s"', value );
      end
      obj.encoded = [ obj.encoded, value ];
    end
    
    function obj = encode_double_array(obj, value)
      
      if ( ~isempty(value) )
        value = arrayfun( @(x) num2str(x), value, 'un', false );
        value = strjoin( value, ',' );
      else value = '';
      end
      obj.encoded = [ obj.encoded, sprintf('[%s]', value) ];
    end
    
    function obj = encode_cell(obj, value)
      
      obj.encoded = [ obj.encoded, '[' ];
      for i = 1:numel(value)
        current = value{i};
        obj.encode_next( current );
        if ( i < numel(value) ), obj.encoded(end+1) = ','; end;
      end
      obj.encoded = [ obj.encoded, ']' ];
    end
    
    function obj = encode_struct(obj, value)
      
      obj.encoded = [ obj.encoded, '{' ];
      fs = fieldnames( value );
      for i = 1:numel(fs)
        current = value.(fs{i});
        obj.encoded = [ obj.encoded, sprintf('"%s":', fs{i}) ];
        obj.encode_next( current );
        if ( i < numel(fs) ), obj.encoded(end+1) = ','; end;
      end
      obj.encoded = [ obj.encoded, '}' ];
    end
  end
  
  methods (Static = true)
    
    function str = encode(values)
      obj = json_encoder();
      obj.encode_next( values );
      str = obj.encoded;
    end
  end
end