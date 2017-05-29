classdef h5_api < handle
  
  properties
    h5_file = NaN;
    CHUNK_SIZE = [1e3 1e2 1e2];
    COMPRESSION_LEVEL = 4;
  end
  
  methods
    
    function obj = h5_api(filename)
      
      %   H5_API -- Instantiate an interface to a .h5 database file.
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
      if ( any(isnan(obj.h5_file)) ), obj.h5_file = filename; end
    end
    
    function create_set(obj, spath, sz, chunk, varargin)
      
      %   CREATE_SET -- Create a numeric dataset.
      %
      %     IN:
      %       - `spath` (char) -- Path to the dataset to create.
      %       - `sz` (double) -- Size of the dataset. If any elements are
      %         Inf, the dataset is extensible in those dimensions.
      %       - `chunk` (double) |OPTIONAL| -- Optionally specify the
      %         chunk-size of the dataset, if the set is to be extensible.
      
      spath = obj.ensure_leading_backslash( spath );
      obj.assert__is_not_set( spath );
      if ( nargin < 4 ), chunk = []; end;
      if ( ~any(isinf(sz)) )
        %   create non-extensible.
        h5create( obj.h5_file, spath, sz, 'deflate', obj.COMPRESSION_LEVEL ...
          , varargin{:} );
        obj.writeatt( spath, 'next_row', -1 );
        return;
      end
      if ( isempty(chunk) )
        chunk = obj.get_chunk_( sz ); 
      else
        assert( numel(chunk) == numel(sz), ['If specifying a chunk' ...
          , ' size, the number of elements must match the number of' ...
          , ' elements in the dataset size vector.'] );
      end
      h5create( obj.h5_file, spath, sz, 'ChunkSize', chunk ...
        , 'deflate', obj.COMPRESSION_LEVEL, varargin{:} );
      obj.writeatt( spath, 'next_row', 0 );
    end
    
    function write(obj, data, sgpath)
      
      %   WRITE -- Write data to a dataset or multiple datasets, depending
      %     on the data-type.
      %
      %     If the dataset(s) already exist, they will be overwritten.
      %     Otherwise, they will be created. Numeric matrices / n-d arrays
      %     are, by default, created as extensible in the first dimension,
      %     and fixed in the remaining dimensions. Other datatypes are, by
      %     default, non-extensible.
      %
      %     Supported datatypes include: Container, double / logical
      %     matrices and n-d arrays, scalar structs, cell arrays of
      %     strings, and char arrays.
      %     
      %     IN:
      %       - `data` (/see above/) -- Data to write.
      %       - `sgpath` (char) -- Path to the set or group in which to
      %         save.
      
      if ( isa(data, 'Container') )
        obj.write_container( data, sgpath );
      elseif ( isnumeric(data) || islogical(data) )
        obj.write_matrix( data, sgpath );
      elseif ( iscellstr(data) )
        obj.write_cellstr( data, sgpath );
      elseif ( isstruct(data) )
        obj.write_struct( data, sgpath );
      elseif ( ischar(data) )
        obj.write_char( data, sgpath );
      else
        error( 'Unsupported data type ''%s''', class(data) );
      end
    end
    
    function write_container(obj, container, gname)
      
      %   WRITE_CONTAINER -- Write a Container object to datasets in a
      %     given group.
      %
      %     If the datasets /data and /labels in the given group already
      %     exist, they will be unlinked (deleted).
      %
      %     IN:
      %       - `container` (Container) -- Container object to save.
      %       - `gname` (char) -- Path to the group in which to save.
      
      if ( ~obj.is_group(gname) ), obj.create_group( gname ); end
      gname = obj.ensure_leading_backslash( gname );      
      snames = { 'data', 'labels', 'indices', 'categories' };
      for i = 1:numel(snames)
        full_sname = obj.fullfile( gname, snames{i} );
        if ( obj.is_set(full_sname) )
          obj.unlink( full_sname );
        end
      end      
      obj.write_container_( container, gname, 1 );
    end
    
    function write_container_(obj, container, gname, start)
      
      %   WRITE_CONTAINER_ -- Private function for writing a Container
      %     object to an extensible dataset, starting from a given row.
      
      data_set_path = [ gname, '/data' ];
      indices_path = [ gname, '/indices' ];
      label_path = [ gname, '/labels' ];
      cat_path = [ gname, '/categories' ];
      
      if ( ~isa(container.labels, 'SparseLabels') )
        container = container.sparse();
      end
      
      data = container.data;
      labels = container.labels;
      indices = uint8( full(labels.indices) );
      categories = labels.categories;
      labs = labels.labels;
      
      if ( ~obj.is_set(data_set_path) )
        obj.write_matrix_( data, data_set_path, 1 );
        obj.write_matrix_( indices, indices_path, 1, [Inf, Inf], [] ...
          , 'FillValue', 0 );
      else
        incoming.indices = indices;
        incoming.labels = labs;
        incoming.categories = categories;
        
        existing.labels = obj.read( label_path );
        existing.categories = obj.read( cat_path );
        
        %   match format of already-stored labels
        [indices, labs, categories] = obj.match_( existing, incoming );
        
        obj.write_matrix_( data, data_set_path, start );
        obj.write_matrix_( indices, indices_path, start );
      end
      
      obj.write( labs, label_path );
      obj.write( categories, cat_path );
      
      obj.writeatt( data_set_path, 'class', 'Container' );
      obj.writeatt( gname, 'class', 'Container' );
      
      if ( isequal(class(container), 'Container') )
        subclass = '';
      else
        subclass = class(container);
      end
      
      obj.writeatt( data_set_path, 'subclass', subclass );
      obj.writeatt( gname, 'subclass', subclass );
    end
    
    function write_matrix(obj, mat, spath)
      
      %   WRITE_MATRIX -- Write a numeric matrix to a dataset, erasing the
      %     current contents.
      %
      %     IN:
      %       - `mat` (double, uint8, logical) -- Data matrix to save.
      %       - `spath` (char) -- Path to the dataset in which to save. The
      %         set will be created if it does not exist.
      
      spath = obj.ensure_leading_backslash( spath );
      if ( obj.is_set(spath) && obj.readatt(spath, 'next_row') > 0 )
        obj.unlink( spath );
      end
      obj.write_matrix_( mat, spath, 1 );
    end
    
    function write_matrix_(obj, data, spath, start, varargin)
      
      %   WRITE_MATRIX_ -- Private function for writing numeric data to a
      %     dataset at a given index.
      %
      %     The dataset will be created if it does not already exist.
      %
      %     IN:
      %       - `data` (double) -- Matrix of data to write.
      %       - `spath` (char) -- Path to the dataset.
      %       - `start` (double) |SCALAR| -- Number specifying the row at
      %         which to start writing data.
      %       - `varargin` (cell) -- Additional inputs to optionally pass
      %         to `create_set()` when creating a new set.
      
      if ( ~obj.is_set(spath) )
        data_sz = size( data );
        if ( isempty(varargin) )
          obj.create_set( spath, [Inf, data_sz(2:end)] );
        else
          obj.create_set( spath, varargin{:} );
        end
        next_row = size( data, 1 ) + 1;
      else next_row = start + size(data, 1);
      end
      [start, count] = obj.get_start_count_( data, start );
      if ( islogical(data) )
        is_logical = 1;
        data = double( data );
      else
        is_logical = 0;
      end
      h5write( obj.h5_file, spath, data, start, count );
      obj.writeatt( spath, 'is_numeric', 1 );
      obj.writeatt( spath, 'next_row', next_row );
      obj.writeatt( spath, 'is_logical', is_logical );
    end
    
    function write_cellstr(obj, data, spath)
      
      %   WRITE_CELLSTR -- Write a 1-d cell-array of strings to a dataset,
      %     erasing the current contents.
      %
      %     Note that cell arrays are not currently extensible.
      %
      %     The bulk of this code was plagiarized from
      %     'HDF5_vlen_string_example.m'
      %
      %     IN:
      %       - `data` (1-d cell array of strings)
      %       - `spath` (char)
      
      obj.assert__iscellstr( data );
      obj.assert__file_exists( obj.h5_file );
      assert( isvector(data), ['If writing a cell array of strings,' ...
        , ' the array must be a vector.'] );
      
      if ( obj.is_set(spath) ), obj.unlink( spath ); end;
      
      spath = obj.ensure_leading_backslash( spath );
      components = obj.path_components( spath );
      
      if ( numel(components) == 0 )
        error( 'The dataset name ''/'' is reserved and invalid.' );
      elseif ( numel(components) > 1 )
        gname = strjoin( components(1:end-1), '/' );
        sname = components{end};
      else
        gname = '/';
        sname = components{end};
      end
      if ( ~obj.is_group(gname) )
        obj.create_group( gname );
      end
      fileattrib( obj.h5_file,'+w' );
      fid = H5F.open( obj.h5_file, 'H5F_ACC_RDWR', 'H5P_DEFAULT' );
      loc_id = H5G.open( fid, gname );
      VLstr_type = H5T.copy( 'H5T_C_S1' );
      H5T.set_size( VLstr_type, 'H5T_VARIABLE' );
      % Create a dataspace for cellstr
      H5S_UNLIMITED = H5ML.get_constant_value( 'H5S_UNLIMITED'  );
      dspace = H5S.create_simple( 1, numel(data), H5S_UNLIMITED );
      % Create a dataset plist for chunking
      plist = H5P.create( 'H5P_DATASET_CREATE' );
      H5P.set_chunk( plist, 2 ); % 2 strings per chunk
      % Create dataset
      dset = H5D.create( loc_id, sname, VLstr_type, dspace, plist );
      % Write data
      H5D.write( dset, VLstr_type, 'H5S_ALL', 'H5S_ALL', 'H5P_DEFAULT', data );
      % Close file & resources
      H5P.close( plist );
      H5T.close( VLstr_type );
      H5S.close( dspace );
      H5D.close( dset );
      H5G.close( loc_id );
      H5F.close( fid );
      % Indicate that this is a cell array of strings
      obj.writeatt( spath, 'class', 'cellstr' );
      obj.writeatt( spath, 'is_numeric', 0 );
      obj.writeatt( spath, 'is_char', 0 );
    end
    
    function write_struct(obj, data, sgpath)
      
      %   WRITE_STRUCT -- Write a scalar struct to the specified group.
      %
      %     IN:
      %       - `sgpath` (char) -- Path to the group / set in which to
      %       save.
      
      obj.assert__isa( data, 'struct', 'the struct data' );
      assert( isscalar(data), 'The struct must be scalar.' );
      if ( obj.is_set(sgpath) || obj.is_group(sgpath) )
        obj.unlink( sgpath );
      end
      obj.create_group( sgpath );
      obj.writeatt( sgpath, 'class', 'struct' );
      obj.writeatt( sgpath, 'is_numeric', 0 );
      fields = fieldnames( data );
      for i = 1:numel( fields )
        current = data.(fields{i});
        full_path = sprintf( '%s/%s', sgpath, fields{i} );
        try 
          obj.write( current, full_path );
        catch err
          fprintf( ['\nThe following error occurred when attempting' ...
            , ' to save struct-data:'] );
          rethrow( err );
        end
      end
    end
    
    function write_char(obj, data, sgpath)
      
      %   WRITE_CHAR -- Write a character array to a given dataset.
      %
      %     IN:
      %       - `data` (char)
      %       - `sgpath` (char) -- Path to the dataset.
      
      obj.assert__isa( data, 'char', 'the char data' );
      data = { data };
      obj.write( data, sgpath );
      obj.writeatt( sgpath, 'is_char', 1 );
    end
    
    function add(obj, data, sgpath)
      
      %   ADD -- Append data to a given dataset or group of datasets.
      %
      %     Datasets will be created if they do not already exist.
      %
      %     IN:
      %       - `data` (Container, double) -- Container object or numeric
      %         matrix to save.
      %       - `gname` (char) -- Group in which to save.
      
      if ( isa(data, 'Container') )
        obj.add_container_( data, sgpath );
      elseif ( isnumeric(data) || islogical(data) )
        obj.add_matrix_( data, sgpath );
      else
        error( 'Unsupported data type ''%s''', class(data) );
      end
    end
    
    function add_container_(obj, container, gname)
      
      %   ADD_CONTAINER_ -- Private function for appending a Container to
      %     an existing group.
      
      obj.assert__is_group( gname );
      gname = obj.ensure_leading_backslash( gname );
      data_set_path = [ gname, '/data' ];
      if ( ~obj.is_set(data_set_path) )
        next_row = 1;
      else next_row = obj.readatt( data_set_path, 'next_row' );
      end
      obj.write_container_( container, gname, next_row );
    end
    
    function add_matrix_(obj, data, spath)
      
      %   ADD_MATRIX_ -- Private function for appending data to an existing
      %     dataset.
      
      spath = obj.ensure_leading_backslash( spath );
      if ( ~obj.is_set(spath) )
        next_row = 1;
      else next_row = obj.readatt( spath, 'next_row' );
      end
      obj.write_matrix_( data, spath, next_row );
    end
    
    %{
        WRITE HELPERS
    %}
    
    function chunk = get_chunk_(obj, sz)
      
      %   GET_CHUNK_ -- Get an appropriate chunk-size for a dataset.
      %
      %     IN:
      %       - `sz` (double) -- Size vector.
      %     OUT:
      %       - `chunk` (double) -- 1xN vector of chunk-values, where each
      %         `chunk`(i) corresponds to each `sz`(i).
      
      dims = numel( sz );
      chunk = obj.CHUNK_SIZE( 1:dims );
      chunk = min( [chunk; sz] );
    end
    
    function [start, count] = get_start_count_(obj, mat, start)
      
      %   GET_START_COUNT_ -- Get appropriate start and count parameter
      %     values given an incoming data matrix.
      %
      %     IN:
      %       - `mat` (double) -- Data matrix
      %       - `start` (double) |SCALAR| -- Number specifying the row at
      %         which to starting writing data.
      
      dims = ndims( mat );
      start = [ start, ones(1, dims-1) ];
      count = size( mat );
    end
    
    function [new_indices, labs, cats] = match_(obj, existing, incoming)
      
      %   MATCH_ -- Private function for rearranging the indices, labels,
      %     and categories of an incoming Container object to match the
      %     format of the already-stored values.
      %
      %     Incoming labels, categories, and indices are rearranged such 
      %     that, for labels shared between incoming and current datasets, 
      %     the incoming indices are appended to the correct column of the 
      %     current indices.
      %
      %     IN:
      %       - `existing` (struct) -- struct containing the existing
      %         'categories' and 'labels'
      %       - `incoming` (struct) -- struct containing the incoming
      %         'categories', 'labels', and 'indices'
      %     OUT:
      %       - `new_indices` (double)
      %       - `labs` (cell array of strings)
      %       - `cats` (cell array of strings)
      
      current_cats =    existing.categories(:);
      current_labels =  existing.labels(:);
      categories =      incoming.categories(:);
      labels =          incoming.labels(:);
      indices =         incoming.indices;
      
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
    
    %{
        READ
    %}
    
    function data = read(obj, sgpath, varargin)
      
      %   READ -- Load a Container object, matrix, struct, or cell array
      %     of strings from the given path.
      %
      %     data = obj.read( '/ex' );
      %     where '/ex' is a path to a dataset called 'ex' loads the
      %     entire contents of 'ex'. 'ex' can be a double or logical matrix 
      %     / n-d array, or a cell array of strings.
      %
      %     data = obj.read( '/ex', index );
      %     instead loads rows of '/ex' at which `index` is true. In this
      %     case, 'ex' must be a double or logical matrix / n-d array.
      %
      %     data = obj.read( '/ex' );
      %     where '/ex' is a path to a group called 'ex' loads the entire
      %     contents of 'ex' according to the class of object stored in
      %     'ex'. If the 'class' attribute of 'ex' is struct, `data` is a
      %     struct. Otherwise, `data` is a Container object.
      %
      %     data = obj.read( '/ex', selector_type, selectors );
      %     where '/ex' is a path to a Container object group called 'ex'
      %     loads the subset of data in 'ex' associated with the given
      %     `selectors` and `selector_type`.
      %
      %     E.g., data = obj.read( '/ex', 'only', {'05/17/2017'} ) loads
      %     only the data associated with the label '05/17/2017'. Valid 
      %     selector types are 'only', 'only not', and 'except'.
      %
      %     IN:
      %       - `sgpath` (char) -- Path to the group or dataset from which
      %         to read.
      %       - `varargin` (cell array) |OPTIONAL| -- Index, or selector 
      %         type / selectors.
      %     OUT:
      %       - `data` (numeric, Container)
      
      sgpath = obj.ensure_leading_backslash( sgpath );
      addtl_inputs_given = ~isempty( varargin );
      err_msg = [ 'Too many input arguments; Can only specify selectors' ...
        , ' if the given path is to a Container group.' ];
      if ( obj.is_group(sgpath) )
        if ( obj.is_container_group(sgpath) )
          if ( ~addtl_inputs_given )
            data = obj.read_container_( sgpath );
          else
            data = obj.read_container_selected_( sgpath, varargin{:} );
          end
        elseif ( obj.is_struct_group(sgpath) )
          assert( ~addtl_inputs_given, err_msg );
          data = obj.read_struct_( sgpath );
        else
          error( 'The group ''%s'' cannot be read from directly.', sgpath );
        end
      else
        obj.assert__is_set( sgpath );
        is_num = obj.readatt( sgpath, 'is_numeric' );
        if ( is_num )
          data = obj.read_matrix_( sgpath, varargin{:} );
        else
          assert( ~addtl_inputs_given, err_msg );
          kind = obj.readatt( sgpath, 'class' );
          switch ( kind )
            case 'cellstr'
              data = obj.read_cellstr_( sgpath );
              is_char = obj.readatt( sgpath, 'is_char' );
              if ( is_char ), data = char( data ); end;
            otherwise
              error( ['Reading data of class ''%s'' is not currently' ...
                , ' supported.'], kind );
          end
        end
      end
    end
    
    function cont = read_container_(obj, gpath)
      
      %   READ_CONTAINER_ -- Read in a Container object from a given group.
      %
      %     IN:
      %       - `gpath` (char) -- Path to the group to read.
      %     OUT:
      %       - `cont` (Container) -- Loaded Container object.
      
      labels = obj.read_labels_( gpath );
      data = obj.read( [gpath, '/data'] );
      cont = Container( data, labels );
    end
    
    function [cont, ind] = read_container_selected_(obj, gpath, selector_type, selectors)
      
      %   READ_CONTAINER_SELECTED_ -- Read a subset of the data in a
      %     Container group associated with the specified selectors and
      %     selector type.
      %
      %     IN:
      %       - `gpath` (char) -- Path to the Container-housing group.
      %       - `selector_type` (char) -- 'only', 'only_not', or 'exclude'
      %       - `selectors` (cell array of strings, char) -- Labels to
      %         select.     
      %     OUT:
      %       - `cont` (Container) -- Loaded Container object.
      %       - `ind` (logical) -- Index used to select rows for `cont`.
      
      labels = obj.read_labels_( gpath );
      ind = obj.get_index_of_selectors( labels, selector_type, selectors );
      assert( any(ind), 'No data matched the given criteria.' );
      data = obj.read_matrix_rows_at_index( ind, [gpath, '/data'] );
      labels = labels.keep( ind );
      cont = Container( data, labels );
    end
    
    function data = read_matrix_rows_at_index(obj, ind, sname)
      
      %   READ_MATRIX_ROWS_AT_INDICES -- Read rows of data at which `ind`
      %     is true.
      %
      %     IN:
      %       - `ind` (logical) -- 1-column matrix specifying elements to
      %         read.
      %       - `sname` (char) -- Path to the dataset to read.
      
      obj.assert__is_set( sname );
      sname = obj.ensure_leading_backslash( sname );
      inds = obj.find_contiguous_indices( ind );
      starts = inds(:, 1);
      counts = inds(:, 2) - inds(:, 1);
      sz = obj.get_set_size( sname );
      dims = numel( sz );
      addtl = ones( 1, dims-1 );
      n_rows = sum( ind );
      data = zeros( [n_rows, sz(2:end)] );
      colons = repmat( {':'}, 1, dims-1 );
      stp = 1;
      for i = 1:numel(starts)
        start = [ starts(i), addtl ];
        count = [ counts(i), sz(2:end) ];
        some_data = obj.read( sname, start, count );
        data( stp:stp+count(1)-1, colons{:} ) = some_data;
        stp = stp + count(1);
      end
    end
    
    function inds = find_contiguous_indices(obj, inds)
      
      %   FIND_CONTIGUOUS_INDICES -- Locate the start and stop points of
      %   	contiguous sequences of logical true values.
      %
      %     IN:
      %       - `inds` (double)
      
      obj.assert__isa( inds, 'logical', 'the indices' );
      if ( issparse(inds) ), inds = full( inds ); end;
      inds = inds(:);
      diffed = diff( inds );
      starts = find( diffed == 1 ) + 1;
      ends = find( diffed == -1 ) + 1;
      if ( inds(1) )
        starts = [ 1; starts ]; 
      end
      if ( inds(end) )
        ends = [ ends; numel(inds)+1 ]; 
      end
      assert( numel(starts) == numel(ends), 'Starts did not match ends.' );
      assert( sum(ends-starts) == sum(inds), 'Counts did not match.' );
      inds = [ starts, ends ];
    end
    
    function ind = get_index_of_selectors(obj, labels, selector_type, selectors)
      
      %   GET_INDEX_OF_SELECTORS -- Determine which elements of a Container
      %     object to select.
      %
      %     IN:
      %       - `labels` (SparseLabels) -- Constructed SparseLabels object.
      %       - `selector_type` (char) -- 'only', 'only_not', or 'exclude'
      %       - `selectors` (cell array of strings, char) -- Labels to
      %         select.
      
      obj.assert__isa( selector_type, 'char', 'the selector type' );
      if ( ~iscell(selectors) ), selectors = { selectors }; end;
      obj.assert__iscellstr( selectors, 'the selectors' );
      obj.assert__isa( labels, 'SparseLabels', 'the the labels object' );
      
      switch ( selector_type )
        case 'only'
          ind = labels.where( selectors );
        case {'only_not', 'only not'}
          ind = ~labels.where( selectors );
        case 'except'
          [~, ind] = labels.remove( selectors );
          ind = ~ind;
        otherwise
          error( 'Unrecognized selector type ''%s''', selector_type );
      end
    end
    
    function data = read_matrix_(obj, spath, start, count)
      
      %   READ_MATRIX_ -- Read in a data matrix from a given dataset.
      %
      %     IN:
      %       - `spath` (char) -- Path to the dataset to read.
      %       - `start` (double) |OPTIONAL| -- Where in the dataset to
      %         start reading.
      %       - `count` (double) |OPTIONAL| -- Number of elements to read
      %         along each dimension.
      %     OUT:
      %       - `data` (double, logical) -- Loaded matrix.
      
      obj.assert__is_set( spath );
      if ( nargin < 4 )
        narginchk( 2, 2 );
        data = h5read( obj.h5_file, spath );
      else
        data = h5read( obj.h5_file, spath, start, count );
      end
      is_logical = obj.readatt( spath, 'is_logical' );
      if ( is_logical ), data = logical( data ); end
    end
    
    function data = read_cellstr_(obj, spath)
      
      %   READ_CELLSTR_ -- Read in a cell array of strings from a given
      %     dataset.
      %
      %     IN:
      %       - `spath` (char) -- Path to the dataset to read.
      %     OUT:
      %       - `data` (cell array of strings)
      
      obj.assert__is_set( spath );
      obj.assert__file_exists( obj.h5_file );
      fid = H5F.open( obj.h5_file, 'H5F_ACC_RDONLY', 'H5P_DEFAULT' );
      VLstr_type = H5T.copy( 'H5T_C_S1' );
      H5T.set_size( VLstr_type, 'H5T_VARIABLE' );
      dset = H5D.open( fid, spath );
      data = H5D.read( dset, VLstr_type, 'H5S_ALL' ...
        , 'H5S_ALL', 'H5P_DEFAULT' );
      H5T.close( VLstr_type );
      H5D.close( dset);
      H5F.close( fid );
    end
    
    function data = read_struct_(obj, sgpath)
      
      %   READ_STRUCT_ -- Read in a struct from a given struct-group.
      %
      %     IN:
      %       - `sgpath` (char) -- Path to the struct-to-read.
      %     OUT:
      %       - `data` (struct)
      
      assert( obj.is_struct_group(sgpath), ['The path ''%s'' is not' ...
        , ' to a struct.'], sgpath );
      sets = obj.get_set_names( sgpath );
      grps = obj.get_group_names( sgpath );
      split = cellfun( @(x) obj.path_components(x), grps, 'un', false );
      grps = cellfun( @(x) x{end}, split, 'un', false );
      all_to_read = [ sets(:); grps(:) ];
      for i = 1:numel(all_to_read)
        full_set = sprintf( '%s/%s', sgpath, all_to_read{i} );
        field = obj.read( full_set );
        data.(all_to_read{i}) = field;
      end
    end
    
    function val = readatt(obj, sname, att)
      
      %   READATT -- Read an attribute from a given dataset or group.
      %
      %     IN:
      %       - `sname` (char) -- Full path to the dataset or group.
      %       - `att` (char) -- Attribute to read.
      
      sname = obj.ensure_leading_backslash( sname );
      obj.assert__is_set_or_group( sname );
      val = h5readatt( obj.h5_file, sname, att );
    end
    
    function writeatt(obj, sname, att_name, att)
      
      %   WRITEATT -- Write an attribute to a given dataset or group.
      %
      %     IN:
      %       - `sname` (char) -- Full path to the dataset or group.
      %       - `att_name` (char) -- Name of the attribute to write to.
      %       - `att` (double, char) -- Value to write.
      
      sname = obj.ensure_leading_backslash( sname );
      obj.assert__is_set_or_group( sname );
      h5writeatt( obj.h5_file, sname, att_name, att );
    end
    
    function rmatt(obj, sname, att)
      
      %   RMATT -- Remove an attribute from a dataset or group.
      %
      %     IN:
      %       - `sname` (char) -- Path to the dataset or group.
      %       - `att` (char) -- Attribute to delete.
      
      sname = obj.ensure_leading_backslash( sname );
      obj.assert__is_set_or_group( sname );
      obj.assert__is_attr( sname, att );
      fid = H5F.open( obj.h5_file, 'H5F_ACC_RDWR','H5P_DEFAULT' );
      gid = H5G.open( fid, sname );
      H5A.delete( gid, att );
      H5G.close( gid );
      H5F.close( fid );
    end
    
    function labels = read_labels_(obj, gname)
      
      %   READ_LABELS_ -- Read labels from a given group.
      %
      %     IN:
      %       - `gname` (char) -- Path to a group containing /data and
      %         /labels datasets.
      %     OUT:
      %       - `labs` (SparseLabels) -- Constructed SparseLabels object.
      
      snames = { 'labels', 'indices', 'categories' };
      
      cellfun( @(x) obj.assert__is_set([gname, '/', x]), snames );
      
      indices =     obj.read( [gname, '/indices'] );
      labs =        obj.read( [gname, '/labels'] );
      categories =  obj.read( [gname, '/categories'] );
      
      labels = SparseLabels();
      labels.labels = labs(:);
      labels.categories = categories(:);
      labels.indices = sparse( indices );      
    end
        
    function unlink(obj, sname)
      
      %   UNLINK -- Delete a group or dataset.
      %
      %     IN:
      %       - `sname` (char) -- Path to the dataset or group to delete.
      
      obj.assert__is_set_or_group( sname );
      fileattrib( obj.h5_file, '+w' );
      fid = H5F.open( obj.h5_file, 'H5F_ACC_RDWR', 'H5P_DEFAULT' );
      H5L.delete( fid, sname, 'H5P_DEFAULT' );
      H5F.close( fid );
    end
    
    function remove(obj, selectors, gpath)
      
      %   REMOVE -- Remove values from a Container group associated with
      %     the given selectors.
      %
      %     IN:
      %       - `selectors` (cell array of strings, char)
      %       - `gpath` (char) -- Path to the Container group.
      
      gpath = obj.ensure_leading_backslash( gpath );
      obj.assert__is_group( gpath );
      
      labs = obj.read_labels_( gpath );
      [~, ind] = labs.remove( selectors );
      if ( ~any(ind) )
        fprintf( '\n No data matched the given selectors ...' );
        return;
      end      
      cont = obj.read_container_( gpath );
      cont = cont.keep( ~ind );      
      obj.write_container( cont, gpath );
    end
    
    function rebuild(obj)
      
      %   REBUILD -- Copy the datasets in the .h5 file to a new .h5 file.
      %
      %     When data in a dataset or group is overwritten, the space eaten
      %     up by the overwritten data is not reclaimed. Rather, the "link"
      %     to the data is overwritten; the data still resides in the file,
      %     but the path to that data is lost. rebuild() copies each
      %     link to a new .h5 file, such that only the linked data is
      %     retained. This reclaims the lost storage space.
      
      current_h5 = obj.h5_file;
      obj.assert__file_exists( current_h5 );
      does_exist = true;
      i = 1;
      while ( does_exist )
        tmp_filename = sprintf( '%s.tmp%d', current_h5, i );
        i = i + 1;
        does_exist = obj.file_exists( tmp_filename );
      end
      obj.create( tmp_filename );
      fileattrib( tmp_filename, '+w' );
      fileattrib( current_h5, '+w' );
      fid = H5F.open( current_h5, 'H5F_ACC_RDWR', 'H5P_DEFAULT' );
      fid_tmp = H5F.open( tmp_filename, 'H5F_ACC_RDWR', 'H5P_DEFAULT' );
      
      info = h5info( current_h5 );
      
      if ( numel(info.Groups > 0) )        
        groups = { info.Groups(:).Name };
        for i = 1:numel(groups)
          copy_one_group( groups{i} );
        end
      end

      H5F.close( fid );
      H5F.close( fid_tmp );
      
      delete( current_h5 );
      movefile( tmp_filename, current_h5 );
      
      function copy_one_group( gname )
        
        %   COPY_ONE_GROUP -- Copy a subgroup to the new file.
        
        ocpl = H5P.create( 'H5P_OBJECT_COPY' );
        lcpl = H5P.create( 'H5P_LINK_CREATE' );
        H5P.set_create_intermediate_group( lcpl, true );
        gid = H5G.open( fid, gname );
        gid_tmp = H5G.open( fid_tmp, '/' );
        H5O.copy( gid, gname, gid_tmp, gname, ocpl, lcpl );
        H5G.close( gid );
        H5G.close( gid_tmp );
        H5P.close( ocpl );
        H5P.close( lcpl );
      end
    end
    
    function tf = contains_(obj, selectors, gpath, kind)
      
      %   CONTAINS_ -- Private. Check whether the Container group contains
      %     the given labels or categories.
      %
      %     IN:
      %       - `selectors` (cell array of strings, char)
      %       - `gpath` (char) -- Path to the Container-housing group.
      %       - `kind` (char) -- 'labels' or 'categories'
      %     OUT:
      %       - `tf` (logical) -- Index of whether each labs(i) is
      %         present in the group.
      
      selectors = obj.ensure_cell( selectors );
      obj.assert__iscellstr( selectors, sprintf('the %s', kind) );
      obj.assert__is_container_group( gpath );
      labs_or_cats = obj.read( obj.fullfile(gpath, kind) );
      tf = cellfun( @(x) any(strcmp(labs_or_cats, x)), selectors );
    end
    
    function tf = contains_labels(obj, labs, gpath)
      
      %   CONTAINS_LABELS -- Check whether the Container group contains the 
      %     given labels.
      %
      %     IN:
      %       - `labs` (cell array of strings, char)
      %       - `gpath` (char) -- Path to the Container-housing group.
      %     OUT:
      %       - `tf` (logical) -- Index of whether each labs(i) is
      %         present in the group.
      
      tf = obj.contains_( labs, gpath, 'labels' );
    end
    
    function tf = contains_categories(obj, cats, gpath)
      
      %   CONTAINS_CATEGORIES -- Check whether the Container group contains
      %     the given categories
      %
      %     IN:
      %       - `labs` (cell array of strings, char)
      %       - `gpath` (char) -- Path to the Container-housing group.
      %     OUT:
      %       - `tf` (logical) -- Index of whether each labs(i) is
      %         present in the group.
      
      tf = obj.contains_( cats, gpath, 'categories' );    
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
    
    function set.COMPRESSION_LEVEL(obj, val)
      
      %   SET.COMPRESSION_LEVEL -- Update the `COMPRESSION_LEVEL` property.
      %
      %     The incoming COMPRESSION_LEVEL must be a number between 0 and
      %     9.
      
      assert( isscalar(val) && val <= 9 && val >= 0 ...
        , 'Specify compression level as a number between 0 and 9.' );
      obj.COMPRESSION_LEVEL = val;
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
    
    function tf = is_struct_group(obj, name)
      
      %   IS_STRUCT_GROUP -- Return whether the given path is to a group
      %     housing a struct.
      %
      %     IN:
      %       - `name` (char)
      %     OUT:
      %       - `tf` (logical) |SCALAR|
      
      obj.assert__isa( name, 'char', 'the group name' );
      name = obj.ensure_leading_backslash( name );
      tf = false;
      try
        kind = obj.readatt( name, 'class' );
        tf = strcmp( kind, 'struct' );
      catch
        return;
      end
    end
    
    function tf = is_container_group(obj, name)
      
      %   IS_CONTAINER_GROUP -- Return whether the given path is to a group
      %     housing a Container object.
      %
      %     IN:
      %       - `name` (char)
      %     OUT:
      %       - `tf` (logical) |SCALAR|
      
      obj.assert__isa( name, 'char', 'the group name' );
      name = obj.ensure_leading_backslash( name );
      tf = false;
      try
        kind = obj.readatt( name, 'class' );
        tf = strcmp( kind, 'Container' );
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
    
    function assert__is_container_group(obj, gpath)
      
      %   ASSERT__IS_CONTAINER_GROUP -- Ensure a given path is to a
      %     Container group.
      
      assert( obj.is_container_group(gpath), ['The specified path ''%s''' ...
        , ' does not house a Container object.'], gpath );
    end
    
    function assert__is_not_set(obj, name)
      
      %   ASSERT__IS_NOT_SET -- Ensure a given set does not already
      %     exist.
      
      assert( ~obj.is_set(name), 'The specified dataset ''%s'' already exists.' ...
        , name );
    end
    
    function assert__is_attr(obj, sname, att)
      
      %   ASSERT__IS_ATTR -- Ensure an attribute exists.
      
      assert( obj.is_attr(sname, att), ['The specified attribute ''%s''' ...
        , ' does not exist.'], att );
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
    
    function disp(obj)
      
      %   DISP -- Overloaded display.
      
      if ( ~any(isnan(obj.h5_file)) && obj.file_exists(obj.h5_file) )
        fprintf( '\n%s connected to ''%s'' with file structure:\n' ...
          , class(obj), obj.h5_file );
        obj.overview();
        fprintf( '\n\n' );
      elseif ( isnan(obj.h5_file) )
        fprintf( '\n%s with no defined .h5 file\n\n\n', class(obj) );
      else
        fprintf( '\n%s associated with the unknown file ''%s''\n\n\n' ...
          , class(obj), obj.h5_file );
      end
    end
    
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
    
    function overview(obj, group_or_set, n_tabs, display_children)
      
      %   OVERVIEW -- Display the structure of the .h5 file.
      %
      %     For datasets, only the size of the set is shown explicitly;
      %     attribute names are listed, but their values are not displayed.
      %
      %     IN:
      %       - `group_or_set` (char) |OPTIONAL| -- Path to the group
      %         or set to display. Defaults to the root group '/'.
      %       - `n_tabs` (double) |OPTIONAL| -- Sets the number of tabs to
      %         prepend to the displayed string. Defaults to 0.
      
      if ( nargin < 2 )
        group_or_set = '/'; 
        n_tabs = 0;
        display_children = true;
      end
      if ( nargin < 3 )
        n_tabs = 0;
        display_children = true;
      end
      if ( ~display_children ), return; end;
      info = h5info( obj.h5_file, group_or_set );
      groups = info.Groups;
      spc = '  ';
      for i = 1:numel(groups)
        grp = groups(i);
        if ( obj.is_container_group(grp.Name) )
          container_class = obj.get_container_class( grp.Name );
          display_group_name = sprintf( '%s Group', container_class );
          display_children = false;
        else
          display_group_name = 'Group';
          display_children = true;
        end
        tabs = repmat( spc, 1, n_tabs );
        tabs = [ '\n' tabs ];
        split = strsplit( grp.Name, '/' );
        to_print = sprintf( '%s ''%s/''', display_group_name, split{end} );
        fprintf( [tabs, to_print] );
        if ( ~isempty(grp.Attributes) )
          print_attrs( grp.Attributes, [tabs, spc] );
        end
        if ( ~isempty(grp.Datasets) && display_children )
          sets = grp.Datasets;
          tabs = [ tabs, spc ];
          for k = 1:numel(sets)
            fprintf( [tabs, 'Dataset ''%s'''], sets(k).Name );
            attrs = sets(k).Attributes;
            print_sz( sets(k).Dataspace.Size, 'Size', [tabs, spc] );
            print_sz( sets(k).Dataspace.MaxSize, 'MaxSize', [tabs, spc] );
            if ( isempty(attrs) ), continue; end;
            print_attrs( attrs, [tabs, spc] );
          end
          fprintf( '\n' );
        end
        if ( ~isempty(grp.Groups) )
          obj.overview( grp.Name, n_tabs+1, display_children );
        end
      end
      function print_attrs( attrs, spc )
        attrs_array = cell( 1, numel(attrs) );
        for j = 1:numel(attrs)
          attrs_array{j} = sprintf( '"%s"', attrs(j).Name );
        end
        attrs_char = strjoin( attrs_array, ', ' );
        fprintf( [spc, 'Attributes: %s'], attrs_char );
      end
      function print_sz( sz, kind, spc )
        sz = arrayfun( @(x) num2str(x), sz, 'un', false );
        sz = [ '[', strjoin(sz, ' '), ']' ];
        fprintf( [spc, '%s: %s'], kind, sz );
      end
    end
    
    function joined = fullfile(obj, varargin)
      
      %   FULLFILE -- Join file parts with a backslash.
      %
      %     IN:
      %       - `varargin` (cell array of strings)
      %     OUT:
      %       - `joined` (char)
      
      obj.assert__iscellstr( varargin, 'the file parts' );
      joined = strjoin( varargin, '/' );
    end
    
    function arr = ensure_cell(obj, arr)
      
      %   ENSURE_CELL -- Ensure an input is a cell array.
      
      if ( ~iscell(arr) ), arr = { arr }; end;
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
    
    function gnames = get_group_names(obj, gname)
      
      %   GET_GROUP_NAMES -- Return an array of subgroup names in the given
      %     group.
      %
      %     IN:
      %       - `gname` (char) -- Group to query.
      %     OUT:
      %       - `gnames` (cell array of strings, {})
      
      gname = obj.ensure_leading_backslash( gname );
      info = h5info( obj.h5_file, gname );
      gnames = {};
      if ( isempty(info.Groups) ), return; end;
      gnames = { info.Groups(:).Name };
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
      obj.assert__is_set_or_group( sname );
      info = h5info( obj.h5_file, sname );
      anames = {};
      if ( isempty(info.Attributes) ), return; end;
      anames = { info.Attributes(:).Name };
    end
    
    function sz = get_set_size(obj, sname)
      
      %   GET_SET_SIZE -- Get the size of a dataset.
      %
      %     IN:
      %       - `sname` (char) -- Dataset to query.
      %     OUT:
      %       - `sz` (double)
      
      sname = obj.ensure_leading_backslash( sname );
      obj.assert__is_set( sname );
      info = h5info( obj.h5_file, sname );
      sz = info.Dataspace.Size;
    end
    
    function kind = get_container_class(obj, gname)
      
      %   GET_CONTAINER_CLASS -- Get the identity of a Container group.
      %
      %     If the 'subclass' attritube of the group is empty, `kind`
      %     is 'Container'. Otherwise, it is the subclass identity.
      %     
      %     IN:
      %       - `gname` (char) -- Path to the Container-housing group.
      %     OUT:
      %       - `kind` (char) -- Class of Container.
      
      gname = obj.ensure_leading_backslash( gname );
      obj.assert__is_container_group( gname );
      subclass = obj.readatt( gname, 'subclass' );
      if ( isempty(subclass) )
        kind = obj.readatt( gname, 'class' );
      else
        kind = subclass;
      end
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
    
    function assert__iscellstr(var, var_kind)
      
      %   ASSERT__ISCELLSTR -- Ensure a variable is a cell array of
      %     strings.
      %
      %     IN:
      %       - `var` (/any/) -- Variable to identify.
      %       - `var_kind` (char) |OPTIONAL| -- Optionally specify what
      %         kind of variable `var` is. E.g., 'filename'. Defaults to
      %         'input'.
      
      if ( nargin < 2 ), var_kind = 'input'; end;
      assert( iscellstr(var), ['Expected %s to be a cell array of strings;' ...
        , ' was a ''%s''.'], var_kind, class(var) );
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