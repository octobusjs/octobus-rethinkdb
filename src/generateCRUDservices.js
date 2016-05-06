import _ from 'lodash';
import Joi from 'joi';

const extractTableName = (namespace) => {
  const lastIndex = namespace.lastIndexOf('.');
  return lastIndex > -1 && namespace.substr(lastIndex + 1);
};

const createTableIfNotExists = (r, conn, tableName) => (
  r.tableList().run(conn).then((tables) => (
    !tables.includes(tableName) && r.tableCreate(tableName).run(conn)
  ))
);

export default (namespace, _options = {}) => {
  const options = Joi.attempt(_options, {
    tableName: Joi.string().default(extractTableName(namespace)),
    r: Joi.func().required(),
    conn: Joi.required(),
    schema: Joi.object(),
    autoCreateTable: Joi.boolean().default(true)
  });

  const { tableName, r, conn, schema, autoCreateTable } = options;
  const getTable = () => r.table(tableName);

  const map = {
    query({ params }) {
      return params(getTable()).run(conn);
    },

    find({ params }) {
      return paramsToQuery(params).run(conn);
    },

    findOne({ params }) {
      return paramsToQuery({ filters: params }).limit(1).run(conn).then((cursor) => (
        cursor.toArray().then((results) => results[0])
      ));
    },

    findById({ params }) {
      return getTable().get(params).run(conn);
    },

    create({ dispatch, params }) {
      return dispatch(`${namespace}.save`, params);
    },

    update({ params }) {
      if (!params.id) {
        throw new Error('You have to provide an id along with the update payload!');
      }

      return getTable().get(params.id).update(_.omit(params, 'id'), {
        returnChanges: true
      }).run(conn).then((result) => {
        const { changes } = result;
        return changes.length ? changes[0].new_val : params;
      });
    },

    validate({ params }) {
      if (!schema) {
        return params;
      }

      if (Array.isArray(params)) {
        return params.map((item) => map.validate({ params: item }));
      }

      return Joi.attempt(_.omit(params, 'id'), schema, {
        convert: true,
        stripUnknown: true
      });
    },

    save({ params, dispatch, emitBefore, emitAfter }) {
      return dispatch(`${namespace}.validate`, params)
        .then((data) => {
          emitBefore(`${namespace}.save`, data);
          return doSave(data);
        })
        .then((result) => {
          emitAfter(`${namespace}.save`, result);
          return result;
        });
    },

    remove({ params = {} }) {
      return paramsToQuery(params).delete({ returnChanges: params.load }).run(conn);
    }
  };

  const paramsToQuery = (params = {}) => {
    let query = getTable();

    if (typeof params === 'string') {
      return query.get(params);
    }

    const { orderBy, filters, limit, skip, fields } = params;


    if (filters) {
      query = query.filter(filters);
    }

    if (orderBy) {
      query = query.orderBy(orderBy);
    }

    if (skip) {
      query = query.skip(skip);
    }

    if (limit) {
      query = query.limit(limit);
    }

    if (fields) {
      query = query.pluck(...fields);
    }

    return query;
  };

  const doInsert = (data) => (
    getTable().insert(data).run(conn).then((result) => {
      if (!Array.isArray(result.generated_keys)) {
        throw new Error(result);
      }

      if (result.generated_keys.length === 1) {
        return Object.assign({}, data, { id: result.generated_keys[0] });
      }

      return data.map((item, index) => (
        Object.assign({}, item, { id: result.generated_keys[index] }
      )));
    })
  );

  const doUpdate = (id, data) => (
    getTable().get(id).update(data, {
      returnChanges: true
    }).run(conn).then((result) => {
      const { changes } = result;
      if (!changes.length) {
        return data;
      }

      return changes[0].new_val;
    })
  );

  const doSave = (data) => (data.id ? doUpdate(data.id, data) : doInsert(data));

  return Promise.resolve(autoCreateTable && createTableIfNotExists(r, conn, tableName))
    .then(() => ({ namespace, map }));
};
