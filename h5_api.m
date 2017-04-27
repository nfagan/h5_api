classdef h5_api < handle
  
  properties
    h5_file = NaN;
    CHUNK_SIZE = [1e3 1e2 1e2];
  end
  
  methods
    
    function obj = h5_api(filename)
      
      %   DSP_H5 -- Instantiate an interface to a .h5 database file.
      %
      %     An error is thrown if the given database file is not found.
      %
      %     IN:
      %       - `filename` (char) -- .h5 file to connect to.
      
      if ( nargin == 0 ), return; end;
      obj.assert__file_exists( filename, 'the .h5 file' );
      obj.h5_file = filename;
    end
    
    function create_group(obj, group_path)
      
      %   CREATE_GROUP -- Create a new group in the current .h5 file.
      %
      %     Paths should begin with '/'. Nested paths will be created
      %     automatically. It is an error to create a group that already
      %     exists.
      %
      %     io.create_group( '/Signals' );
      %     io.create_group( '/Signals/NonCommonAveraged' );
      %
      %     IN:
      %       - `group_path` (char)
      
      obj.assert__h5_is_defined();
      obj.assert__file_exists( obj.h5_file, 'the .h5 file' );
      
      group_path = obj.path_components( group_path );
      for i = 1:numel(group_path)
        current = strjoin( group_path(1:i), '/' );
        if ( i == numel(group_path) )
          obj.assert__is_not_group( current );
        end
        if ( obj.is_group( current ) ), continue; end;
        obj.create_group_( current );
      end
    end
    
    function create_group_(obj, one_group)
      
      %   CREATE_GROUP_ -- Private function for creating a group in the
      %     current .h5 file.
      %
      %     IN:
      %       - `one_group` (char) -- Current full path of the group to
      %         create.
      
      plist = 'H5P_DEFAULT';
      fid = H5F.open( obj.h5_file, 'H5F_ACC_RDWR', plist );
      gid = H5G.create( fid, one_group, plist, plist, plist );
      H5G.close( gid );
      H5F.close( fid );
    end
    
    function create(obj, filename)
      
      %   CREATE -- Create a new .h5 file.
      %
      %     IN:
      %       - `filename` (char) -- Filename to create.
      
      exists = obj.file_exists( filename );
      assert( ~exists, 'The .h5 file ''%s'' already exists.', filename );
      H5F.create( filename );
      if ( isnan(obj.h5_file) ), obj.h5_file = filename; end
    end
    
    function write(obj, container, gname)
      
      %   WRITE -- Write data in a Container object to a given group.
      %
      %     If the datasets /data and /labels in the given group already
      %     exist, they will be unlinked (deleted).
      
      obj.assert__isa( container, 'Container' );
      obj.assert__is_group( gname );
      gname = obj.ensure_leading_backslash( gname );
      
      data_set_path = [ gname, '/data' ];
      label_set_path = [ gname, '/labels' ];
      
      if ( obj.is_set( data_set_path ) ), obj.unlink( data_set_path ); end
      if ( obj.is_set( label_set_path ) ), obj.unlink( label_set_path ); end
      
      obj.write_( container, gname, 1 );
    end
    
    function write_(obj, container, gname, start)
      
      %   WRITE_ -- Private function for writing data to an extendible
      %     dataset, starting from a given row.
      
      data_set_path = [ gname, '/data' ];
      label_set_path = [ gname, '/labels' ];
      
      if ( ~isa(container.labels, 'SparseLabels') )
        container = container.sparse();
      end
      
      data = container.data;
      labels = container.labels;
      indices = uint8( full(labels.indices) );
      categories = labels.categories;
      labs = labels.labels;
      
      if ( ~obj.is_set(data_set_path) )
        make_container_set();
        next_row = size( data, 1 ) + 1;
      else
        current_row = h5readatt( obj.h5_file, data_set_path, 'next_row' );
        next_row = current_row + size( data, 1 );
        [indices, labs, categories] = obj.match_( label_set_path, indices, labs, categories );
      end
      
      [data_start, data_count] = get_start_count( data, start );
      [label_start, label_count] = get_start_count( indices, start );
            
      h5write( obj.h5_file, data_set_path, data, data_start, data_count );
      h5write( obj.h5_file, label_set_path, indices, label_start, label_count );
      
      categories = json( 'encode', categories );
      labs = json( 'encode', labs );
      
      h5writeatt( obj.h5_file, label_set_path, 'categories', categories );
      h5writeatt( obj.h5_file, label_set_path, 'labels', labs );
      
      h5writeatt( obj.h5_file, data_set_path, 'class', class(container) );
      h5writeatt( obj.h5_file, data_set_path, 'next_row', next_row );
      
      function [sz, chunk] = get_sz_chunk( mat )        
        sz = size( mat );
        dims = numel( sz );
        sz(1) = Inf;
        chunk = obj.CHUNK_SIZE( 1:dims );
        chunk = min( [chunk; sz] );
      end 
      function [start, count] = get_start_count( mat, start )
        dims = ndims( mat );
        start = [ start, ones(1, dims-1) ];
        count = size( mat );
      end
      function make_container_set()
        [data_sz, data_chunk] = get_sz_chunk( data );
        [~, label_chunk] = get_sz_chunk( indices );
        label_sz = Inf( 1, 2 );
        h5create( obj.h5_file, data_set_path, data_sz, 'ChunkSize', data_chunk );
        h5create( obj.h5_file, label_set_path, label_sz ...
          , 'ChunkSize', label_chunk, 'FillValue', 0 );
      end
    end
    
    function [new_indices, labs, cats] = ...
        match_(obj, label_set_path, indices, labels, categories)
      
      %   MATCH_ -- Private function for rearranging the indices, labels,
      %     and categories of an incoming Container object to match the
      %     format of the already-stored values.
      %
      %     `match_()` reads the categories and labels attributes of the
      %     current labels dataset. Incoming labels, categories, and
      %     indices are rearranged such that, for labels shared between
      %     incoming and current datasets, the incoming indices are
      %     appended to the correct column of the current indices.
      %
      %     IN:
      %       - `label_set_path` (char) -- Path to the labels dataset to
      %         match.
      %       - `indices` (double) -- Array of indices, converted from
      %         sparse logical.
      %       - `labels` (cell array of strings)
      %       - `categories` (cell array of strings)
      %     OUT:
      %       - `new_indices` (double)
      %       - `labs` (cell array of strings)
      %       - `cats` (cell array of strings)
      
      current_cats = json( 'parse', obj.readatt(label_set_path, 'categories') );
      current_labels = json( 'parse', obj.readatt(label_set_path, 'labels') );
      current_cats = current_cats(:);
      current_labels = current_labels(:);
      labels = labels(:);
      categories = categories(:);
      
      assert( isequal(unique(current_cats), unique(categories)) ...
        , ['The categories must match between the stored object and' ...
        , ' the incoming object.'] );

      shared_labels = intersect( current_labels, labels );
      unique_to_incoming = setdiff( labels, current_labels );
      unique_to_current = setdiff( current_labels, labels );
      n_shared = numel( shared_labels );
      n_incoming = numel( unique_to_incoming );
      n_current = numel( unique_to_current );
      n_total = n_shared + n_incoming + n_current;
      new_indices = zeros( size(indices,1), n_total );
      labs = cell( 1, size(new_indices, 2) );
      cats = cell( size(labs) );
      if ( n_shared > 0 )
        current_inds = cellfun( @(x) find(strcmp(current_labels, x)), shared_labels );
        new_inds = cellfun( @(x) find(strcmp(labels, x)), shared_labels );
        new_indices( :, current_inds ) = indices( :, new_inds );
        cats( current_inds ) = categories( new_inds );
        labs( current_inds ) = labels( new_inds );
      end
      if ( n_incoming > 0 )
        incoming_inds = cellfun( @(x) find(strcmp(labels, x)), unique_to_incoming );
        col = n_shared + n_current + 1;
        new_indices( :, col:end ) = indices( :, incoming_inds );
        cats( col:end ) = categories( incoming_inds );
        labs( col:end ) = labels( incoming_inds );
      end
      if ( n_current > 0 )
        current_inds = cellfun( @(x) find(strcmp(current_labels, x)), unique_to_current );
        cats( current_inds ) = current_cats( current_inds );
        labs( current_inds ) = unique_to_current;
      end
    end
    
    function add(obj, container, gname)
      
      %   ADD -- Append data in a Container object to a given group.
      %
      %     A new dataset will be created if it does not already exist.
      %
      %     IN:
      %       - `container` (Container) -- Container object to save.
      %       - `gname` (char) -- Group in which to save.
      
      obj.assert__isa( container, 'Container' );
      obj.assert__is_group( gname );
      gname = obj.ensure_leading_backslash( gname );
      data_set_path = [ gname, '/data' ];
      if ( ~obj.is_set(data_set_path) )
        next_row = 1;
      else next_row = h5readatt( obj.h5_file, data_set_path, 'next_row' );
      end
      obj.write_( container, gname, next_row );
    end
    
    function cont = read(obj, gname)
      
      %   READ -- Load a Container from the given group.
      %
      %     IN:
      %       - `gname` (char) -- Path to the group housing /data and
      %         /labels datasets.
      
      obj.assert__is_group( gname );
      gname = obj.ensure_leading_backslash( gname );
      obj.assert__is_set( [gname, '/data'] );
      labels = obj.read_labels_( gname );
      
      data = h5read( obj.h5_file, [gname, '/data'] );     
      cont = Container( data, labels );
    end
    
    function val = readatt(obj, sname, att)
      
      %   READATT -- Read an attribute from a given dataset.
      %
      %     IN:
      %       - `sname` (char) -- Full path to the dataset.
      %       - `att` (char) -- Attribute to read.
      
      obj.assert__is_set( sname );
      val = h5readatt( obj.h5_file, sname, att );
    end
    
    function writeatt(obj, sname, att_name, att)
      
      %   WRITEATT -- Write an attribute to a given dataset.
      %
      %     IN:
      %       - `sname` (char) -- Full path to the dataset.
      %       - `att_name` (char) -- Name of the attribute to write to.
      %       - `att` (double, char) -- Value to write.
      
      obj.assert__is_set( sname );
      h5writeatt( obj.h5_file, sname, att_name, att );
    end
    
    function encodeatt(obj, sname, att_name, att)
      
      %   ENCODEATT -- Write an attribute to a given dataset, after
      %     encoding it to a JSON string.
      %
      %     IN:
      %       - `sname` (char) -- Full path to the dataset.
      %       - `att_name` (char) -- Name of the attribute to write to.
      %       - `att` (double, char) -- Value to write.
      
      att = json( 'encode', att );
      obj.writeatt( sname, att_name, att );
    end
    
    function att = decodeatt(obj, sname, att)
      
      %   DECODEATT -- Read an attribute from a given dataset, and parse it
      %     into a Matlab variable.
      %
      %     IN:
      %       - `sname` (char) -- Full path to the dataset.
      %       - `att` (char) -- Attribute to read.
      
      att = obj.readatt( sname, att );
      att = json( 'parse', att );
    end
    
    function att = parseatt(obj, sname, att)
      
      %   PARSEATT -- Alias for `decodeatt()`.
      
      att = obj.decodeatt( sname, att );
    end
    
    function labels = read_labels_(obj, gname)
      
      %   READ_LABELS_ -- Read labels from a given group.
      %
      %     IN:
      %       - `gname` (char) -- Path to a group containing /data and
      %         /labels datasets.
      %     OUT:
      %       - `labs` (SparseLabels) -- Constructed SparseLabels object.
      
      gname = [ gname, '/labels' ];
      obj.assert__is_set( gname );
      indices = h5read( obj.h5_file, gname );
      categories = h5readatt( obj.h5_file, gname, 'categories' );
      labs = h5readatt( obj.h5_file, gname, 'labels' );
      
      categories = json( 'parse', categories );
      categories = categories(:);
      labs = json( 'parse', labs );
      labs = labs(:);
      
      labels = SparseLabels();
      labels.labels = labs;
      labels.categories = categories;
      labels.indices = sparse( logical(indices) );      
    end
        
    function unlink(obj, sname)
      
      %   UNLINK -- 'Delete' a dataset. Does not recover any space!
      %
      %     IN:
      %       - `sname` (char) -- Path to the dataset to delete.
      
      obj.assert__is_set( sname );
      fid = H5F.open( obj.h5_file, 'H5F_ACC_RDWR', 'H5P_DEFAULT' );
      H5L.delete( fid, sname, 'H5P_DEFAULT' );
      H5F.close(fid);
    end
    
    %{
        PROPERTY VALIDATION
    %}
    
    function set.h5_file(obj, filename)
      
      %   SET.H5_FILE -- Update the `h5_file` property.
      %
      %     The incoming .h5_file must be exist.
      
      obj.assert__file_exists( filename, 'the .h5 file' );
      obj.h5_file = filename;
    end
    
    function tf = is_set(obj, name)
      
      %   IS_SET -- Return whether a given path is to an existing dataset.
      %
      %     IN:
      %       - `name` (char)
      %     OUT:
      %       - `tf` (logical) |SCALAR|
      
      obj.assert__isa( name, 'char', 'the dataset name' );
      name = obj.ensure_leading_backslash( name );
      tf = false;
      try
        info = h5info( obj.h5_file, name );
        tf = ~isfield( info, 'Datasets' );
      catch
        return;
      end
    end
    
    function tf = is_attr(obj, sname, att)
      
      %   IS_ATTR -- Return whether a given attribute is an attribute in a
      %     given dataset.
      %
      %     IN:
      %       - `sname` (char) -- Path to the dataset to query.
      %       - `att` (char) -- Attribute to search for.
      %     OUT:
      %       - `tf` (logical) |SCALAR|
      
      obj.assert__isa( att, 'char', 'the attribute to query' );
      atts = obj.get_attr_names( sname );
      tf = any( strcmp(atts, att) );
    end
    
    function tf = is_group(obj, name)
      
      %   IS_GROUP -- Return whether a given path is to an existing group.
      %
      %     IN:
      %       - `name` (char)
      %     OUT:
      %       - `tf` (logical) |SCALAR|
      
      obj.assert__isa( name, 'char', 'the group name' );
      name = obj.ensure_leading_backslash( name );
      tf = false;
      try
        info = h5info( obj.h5_file, name );
        tf = isfield( info, 'Datasets' );
      catch
        return;
      end
    end
    
    function assert__is_set(obj, name)
      
      %   ASSERT__IS_SET -- Ensure a dataset exists.
      
      assert( obj.is_set(name), ['The specified dataset ''%s'' does not' ...
        , ' exist.'], name );
    end
    
    function assert__is_group(obj, name)
      
      %   ASSERT__IS_GROUP -- Ensure a group exists.
      
      assert( obj.is_group(name), ['The specified group ''%s'' does not' ...
        , ' exist.'], name );
    end
    
    function assert__is_set_or_group(obj, name)
      
      %   ASSERT__IS_SET_OR_GROUP -- Ensure a given path is a valid path to
      %     a group or dataset.
      
      obj.assert__isa( name, 'char', 'the set or group name' );
      assert( obj.is_group(name) || obj.is_set(name), ['The specified path' ...
        , ' ''%s'' is not a recognized group or dataset.'], name );
    end
    
    function assert__is_not_group(obj, name)
      
      %   ASSERT__IS_NOT_GROUP -- Ensure a given group does not already
      %     exist.
      
      assert( ~obj.is_group(name), 'The specified group ''%s'' already exists.' ...
        , name );
    end
    
    function assert__is_not_set(obj, name)
      
      %   ASSERT__IS_NOT_SET -- Ensure a given set does not already
      %     exist.
      
      assert( obj.is_set(name), 'The specified dataset ''%s'' already exists.' ...
        , name );
    end
    
    function assert__h5_is_defined(obj)
      
      %   ASSERT__H5_IS_DEFINED -- Ensure a .h5 file has been defined.
      %
      %     Note that this function does not and is not intended to ensure
      %     that a defined .h5 file exists.
      
      assert( ~any(isnan(obj.h5_file)), 'No current .h5 file has been defined.' );
    end
    
    %{
        UTIL
    %}
    
    function list(obj, group_or_set)
      
      %   LIST -- Display the contents of part or all of the current .h5
      %     file.
      %
      %     IN:
      %       - `group_or_set` (char) |OPTIONAL| -- Optionally specify the
      %       group or dataset whose contents are to be displayed.
      
      obj.assert__file_exists( obj.h5_file, 'the .h5 file' );
      if ( nargin < 2 )
        h5disp( obj.h5_file ); return;
      end
      obj.assert__is_set_or_group( group_or_set );
      h5disp( obj.h5_file, group_or_set );
    end
    
    function snames = get_set_names(obj, gname)
      
      %   GET_SET_NAMES -- Return an array of dataset names in the given
      %     group.
      %
      %     IN:
      %       - `gname` (char) -- Group to query.
      %     OUT:
      %       - `snames` (cell array of strings, {})
      
      gname = obj.ensure_leading_backslash( gname );
      info = h5info( obj.h5_file, gname );
      snames = {};
      if ( isempty(info.Datasets) ), return; end;
      snames = { info.Datasets(:).Name };
    end
    
    function anames = get_attr_names(obj, sname)
      
      %   GET_ATTR_NAMES -- Return an array of attribute names in the given
      %     dataset.
      %
      %     IN:
      %       - `sname` (char) -- Dataset to query.
      %     OUT:
      %       - `anames` (cell array of strings, {})
      
      sname = obj.ensure_leading_backslash( sname );
      obj.assert__is_set( sname );
      info = h5info( obj.h5_file, sname );
      anames = {};
      if ( isempty(info.Attributes) ), return; end;
      anames = { info.Attributes(:).Name };
    end
    
    function split = path_components(obj, str)
      
      %   PATH_COMPONENTS -- Separate a valid path string into
      %     sub-components.
      %
      %     IN:
      %       - `str` (char)
      %     OUT:
      %       - `split` (cell array of strings)
      
      obj.assert__isa( str, 'char', 'the path string' );
      split = strsplit( str, '/' );
      split( cellfun(@isempty, split) ) = [];
    end
    
    function str = ensure_leading_backslash(obj, str)
      
      %   ENSURE_LEADING_BACKSLASH -- Ensure a string begins with '/'
      %
      %     IN:
      %       - `str` (char)
      %     OUT:
      %       - `str` (char)
      
      obj.assert__isa( str, 'char', 'the path string' );
      if ( ~isequal(str(1), '/') ), str = [ '/' str ]; end;
    end
  end
  
  
  methods (Static = true)    
    function tf = file_exists(filename)
      
      %   FILE_EXISTS -- True if the given filename is a path to an
      %     existing file.
      
      h5_api.assert__isa( filename, 'char', 'the .h5 filename' );
      tf = exist( filename, 'file') == 2;
    end
    
    function assert__isa(var, kind, var_kind)
      
      %   ASSERT__ISA -- Ensure a variable is of a given type.
      %
      %     IN:
      %       - `var` (/any/) -- Variable to identify.
      %       - `kind` (char) -- Expected class of `var`.
      %       - `var_kind` (char) |OPTIONAL| -- Optionally specify what
      %         kind of variable `var` is. E.g., 'filename'. Defaults to
      %         'input'.
      
      if ( nargin < 3 ), var_kind = 'input'; end;
      assert( isa(var, kind), 'Expected %s to be a ''%s''; was a ''%s''.' ...
        , var_kind, kind, class(var) );
    end
    
    function assert__file_exists(filename, kind)
      
      %   ASSERT__FILE_EXISTS -- Ensure a file exists.
      %
      %     IN:
      %       - `filename` (char)
      %       - `kind` (char) |OPTIONAL| -- Optionally indicate the kind of
      %         file that was expected to exist. Defaults to an empty
      %         string.
      
      if ( nargin < 2 ), kind = ''; end;
      assert( h5_api.file_exists(filename), ['The specified %s file ''%s'' does' ...
        , ' not exist.'], kind, filename );
    end   
  end
  
end

% function cont = read_selected(obj, gname, selectors)
%       
%       obj.assert__is_group( gname );
%       gname = obj.ensure_leading_backslash( gname );
%       labels = obj.read_labels_( gname );
%       
%       ind = labels.where( selectors );
%       numeric = find( ind );
%       assert( ~isempty(numeric), 'No rows matched the specified criteria.' );
%       non_contiguous = [ 1; diff( numeric ) ~= 1 ];
%       non_contiguous = find( non_contiguous );
%       
%       if ( non_contiguous(end) ~= sum(ind) )
%         non_contiguous(end+1) = sum(ind);
%       end
%       
%       data = zeros( sum(ind), 129, 41 );
%       stp = 1;
%       for i = 1:numel(non_contiguous)-1
%         current_inds = numeric( non_contiguous(i) ) : numeric(non_contiguous(i+1)-1);
%         start = current_inds(1);
%         count = numel( current_inds );
%         count = [ count, Inf, Inf ];
%         data( stp:stp+count(1)-1, :, : ) = h5read( obj.h5_file, [gname, '/data'], [start, 1, 1], count );
%         stp = stp + count(1);
%       end      
%       labels = labels.keep( ind );
%       cont = Container( data, labels );
%     end