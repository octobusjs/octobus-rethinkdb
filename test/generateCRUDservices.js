import r from 'rethinkdb';
import Joi from 'joi';
import { expect } from 'chai';
import sinon from 'sinon';
import { createEventDispatcher } from 'octobus.js';
import { generateCRUDServices } from '../src';

const userSchema = {
  firstName: Joi.string().required(),
  lastName: Joi.string().required(),
  email: Joi.string().email(),
  role: Joi.string(),
  age: Joi.number(),
  birthdate: {
    year: Joi.number(),
    day: Joi.number()
  },
  hobbies: Joi.array().items(Joi.string())
};

const createDatabaseIfNotExists = (conn) => (
  r.dbList().run(conn).then((databases) => {
    if (!Array.isArray(databases) || !databases.includes('test')) {
      return r.dbCreate('test').run(conn);
    }

    return conn.use('test');
  })
);

describe('generateCRUDServices', () => {
  let dispatcher;
  let conn;

  before(() => (
    r.connect({}).then((_conn) => {
      conn = _conn;
      return createDatabaseIfNotExists(conn).then(() => (
        r.tableDrop('User')
      ));
    }).error((err) => {
      throw err;
    })
  ));

  beforeEach(() => {
    dispatcher = createEventDispatcher();

    return generateCRUDServices('entity.User', {
      r, conn,
      schema: userSchema,
      indexes: {
        email: 'email',
        fullname: ['firstName', 'lastName'],
        birthdateYear: 'birthdate.year',
        hobbies: {
          multi: true
        },
        summary: (_r) => (row) => _r.add(row('firstName'), '_', row('lastName'), '_', row('age'))
      }
    }).then(({ namespace, map }) => {
      dispatcher.subscribeMap(namespace, map);
    });
  });

  afterEach(() => r.table('User').delete().run(conn));

  after(() => (
    conn.close().error((err) => {
      throw err;
    })
  ));

  it('should call the create hooks', () => {
    const before = sinon.spy();
    const after = sinon.spy();
    dispatcher.onBefore('entity.User.create', before);
    dispatcher.onAfter('entity.User.create', after);
    return dispatcher.dispatch('entity.User.create', {
      firstName: 'John',
      lastName: 'Doe'
    }).then(() => {
      expect(before).to.have.been.calledOnce();
      expect(after).to.have.been.calledOnce();
    });
  });

  it('should create a new record', () => (
    dispatcher.dispatch('entity.User.create', {
      firstName: 'John',
      lastName: 'Doe'
    }).then((result) => {
      expect(result.id).to.exist();
      expect(result.firstName).to.equal('John');
      expect(result.lastName).to.equal('Doe');
    })
  ));

  it('should create an array of records', () => (
    dispatcher.dispatch('entity.User.create', [{
      firstName: 'John1',
      lastName: 'Doe1'
    }, {
      firstName: 'John2',
      lastName: 'Doe2'
    }, {
      firstName: 'John3',
      lastName: 'Doe3'
    }]).then((results) => {
      expect(results).to.have.lengthOf(3);
      expect(results[0].lastName).to.equal('Doe1');
      expect(results[1].firstName).to.equal('John2');
      expect(results[2].id).to.exist();
    })
  ));

  it('should find an existing record by id', () => (
    dispatcher.dispatch('entity.User.create', {
      firstName: 'John',
      lastName: 'Doe'
    }).then((createdUser) => {
      dispatcher.dispatch('entity.User.findById', createdUser.id)
        .then((foundUser) => {
          expect(foundUser.id).to.equal(createdUser.id);
          expect(foundUser.firstName).to.equal('John');
          expect(foundUser.lastName).to.equal('Doe');
        });
    })
  ));

  it('should remove an existing record', () => (
    dispatcher.dispatch('entity.User.create', {
      firstName: 'John',
      lastName: 'Doe'
    }).then((createdUser) => (
      dispatcher.dispatch('entity.User.remove', createdUser.id).then(() => (
        dispatcher.dispatch('entity.User.findById', createdUser.id)
          .then((result) => {
            expect(result).to.be.null();
          })
      ))
    ))
  ));

  it('should update an existing record', () => (
    dispatcher.dispatch('entity.User.create', {
      firstName: 'John',
      lastName: 'Doe'
    }).then((createdUser) => (
      dispatcher.dispatch('entity.User.update', Object.assign({}, createdUser, {
        lastName: 'Donovan'
      })).then((updatedUser) => {
        expect(updatedUser.id).to.equal(createdUser.id);
        expect(updatedUser.firstName).to.equal('John');
        expect(updatedUser.lastName).to.equal('Donovan');
      })
    ))
  ));

  it('should update multiple records');

  it('should find one record', () => (
    dispatcher.dispatch('entity.User.create', {
      firstName: 'John',
      lastName: 'Doe'
    }).then((createdUser) => (
      dispatcher.dispatch('entity.User.findOne', {
        firstName: 'John'
      }).then((foundUser) => {
        expect(foundUser.id).to.equal(createdUser.id);
        expect(foundUser.lastName).to.equal('Doe');
      })
    ))
  ));

  it('should find records');

  it('should create a simple index', () => (
    r.table('User').indexList().run(conn).then((indexes) => {
      expect(indexes).to.include('email');
    })
  ));

  it('should create an index based on a nested field', () => (
    r.table('User').indexList().run(conn).then((indexes) => {
      expect(indexes).to.include('birthdateYear');

      return r.table('User').indexStatus().run(conn).then((statuses) => {
        const index = statuses.find((status) => status.index === 'birthdateYear');

        expect(index).to.be.ok();
        expect(index.query).to
          .match(/r.row\("birthdate"\)\("year"\)/);
      });
    })
  ));

  it('should create a multi index', () => (
    r.table('User').indexList().run(conn).then((indexes) => {
      expect(indexes).to.include('hobbies');

      return r.table('User').indexStatus().run(conn).then((statuses) => {
        const index = statuses.find((status) => status.index === 'hobbies');

        expect(index).to.be.ok();
        expect(index.multi).to.be.true();
      });
    })
  ));

  it('should create an index based on a function', () => (
    r.table('User').indexList().run(conn).then((indexes) => {
      expect(indexes).to.include('summary');

      return r.table('User').indexStatus().run(conn).then((statuses) => {
        const index = statuses.find((status) => status.index === 'summary');

        expect(index).to.be.ok();
        expect(index.query).to.match(
          /r.add\(var[\d]\("firstName"\), "_", var[\d]\("lastName"\), "_", var[\d]\("age"\)\)/
        );
      });
    })
  ));

  it('should create a compound index', () => (
    r.table('User').indexList().run(conn).then((indexes) => {
      expect(indexes).to.include('fullname');

      return r.table('User').indexStatus().run(conn).then((statuses) => {
        const index = statuses.find((status) => status.index === 'fullname');

        expect(index).to.be.ok();
        expect(index.query).to
          .match(/r.expr\(\[r.row\("firstName"\), r.row\("lastName"\)\]\)/);
      });
    })
  ));
});
