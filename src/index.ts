import { SessionProvider, ISession, Session } from '@spinajs/acl';
import { IContainer, Autoinject } from '@spinajs/di';
import { Configuration } from '@spinajs/configuration';
import * as AWS from 'aws-sdk';
import { Logger } from '@spinajs/log';
import { Log } from '@spinajs/log';
import { InvalidOperation } from '@spinajs/exceptions';
import * as fs from 'fs';

export class DynamoDBSessionStore extends SessionProvider {
  @Logger()
  protected Log: Log;

  @Autoinject()
  protected Configuration: Configuration;

  protected DynamoDb: AWS.DynamoDB;

  protected TableName: string;

  public async resolveAsync(_: IContainer): Promise<void> {
    const credentials = this.Configuration.get<string>('acl.session.dynamodb.aws_config_file');
    const region = this.Configuration.get<string>('acl.session.dynamodb.region');

    this.TableName = this.Configuration.get<string>('acl.session.dynamodb.table');

    if (!fs.existsSync(credentials)) {
      throw new InvalidOperation('no aws config file');
    }

    AWS.config.loadFromPath(credentials);
    AWS.config.update({ region });

    this.DynamoDb = new AWS.DynamoDB({ apiVersion: '2012-08-10' });
  }

  public async restoreSession(sessionId: string): Promise<ISession> {
    const params = {
      TableName: this.TableName,
      Key: {
        session_id: { S: sessionId },
      },
    };

    const result: any = await new Promise((res, rej) => {
      this.DynamoDb.getItem(params, (err, data) => {
        if (err) {
          rej(err);
          return;
        }

        if (!data.Item) {
          res(null);
        }

        res({
          creation: data.Item.creation.S,
          expiration: data.Item.expiration.N,
          session_id: data.Item.session_id.S,
          value: JSON.parse(data.Item.value.S),
        });
      });
    });

    if (!result) {
      return null;
    }

    const session = new Session({
      SessionId: sessionId,
      Data: result.value.Data,
      Expiration: new Date(result.expiration * 1000),
      Creation: new Date(result.creation),
    });

    if (session.Expiration < new Date()) {
      return null;
    }

    return session;
  }

  public async deleteSession(sessionId: string): Promise<void> {
    const params = {
      TableName: this.TableName,
      Key: {
        session_id: { S: sessionId },
      },
    };

    await new Promise((res, rej) => {
      this.DynamoDb.deleteItem(params, (err, _) => {
        if (err) {
          rej(err);
          return;
        }
        res();
      });
    });
  }

  public async updateSession(session: ISession): Promise<void> {
    const params = {
      TableName: this.TableName,
      Item: {
        session_id: { S: session.SessionId },
        value: {
          S: JSON.stringify({ Data: session.Data }),
        },
        creation: { S: session.Creation.toISOString() },
        expiration: { N: `${this._toUnix(session.Expiration)}` },
      },
    };

    await new Promise((res, rej) => {
      this.DynamoDb.putItem(params, (err, _) => {
        if (err) {
          rej(err);
          return;
        }
        res();
      });
    });
  }

  public async refreshSession(session: string | ISession): Promise<void> {
    let sInstance: ISession = null;

    if (typeof session === 'string') {
      sInstance = await this.restoreSession(session);
    } else {
      sInstance = session;
    }

    if (session) {
      sInstance.extend(this.Configuration.get<number>('acl.session.expiration', 10));
      await this.updateSession(sInstance);
    }
  }

  private _toUnix(date: Date): number {
    return Math.round(date.getTime() / 1000);
  }
}
